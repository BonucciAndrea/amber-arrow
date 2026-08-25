/* amberd.c  -  a TCP query server for the Amber engine.
 *
 * WHY THIS EXISTS. The Amber engine speaks sockets as a CLIENT -- ipc.k's
 * hopen/hsend/hsync sit on the connect() in src/i.c -- but it has no listener,
 * and it should not grow one: a listener is a policy decision (which port, which
 * framing, which authentication, which concurrency model) and the engine's job
 * is to be a runtime. So the listener lives out here, in ~450 lines that link
 * libamber.so and touch nothing in src/.
 *
 * Three satellites need a running Amber to talk to, and all three talk to this:
 * the Grafana datasource backend, the VS Code language server, and amber-flight
 * in its --remote mode.
 *
 * CONCURRENCY. The engine is single-threaded, so this is a single-threaded
 * event loop over poll(): many connections, one query at a time, shared global
 * state. That is not a compromise -- it is the same model kdb+ uses, and it is
 * the only one that lets two clients see each other's writes. Forking per
 * connection would give parallelism and take away the shared table you were
 * connecting in order to query. A long query blocks the loop; that is visible,
 * predictable, and fixable by running more amberd processes over a partitioned
 * database, which is also what kdb+ does.
 *
 * SECURITY. There is none, deliberately and visibly: a client that can connect
 * can evaluate arbitrary Amber, which includes reading files and spawning
 * processes. This binds to 127.0.0.1 unless told otherwise, refuses to bind a
 * non-loopback address without --i-know-this-is-unauthenticated, and supports
 * --readonly to reject anything that looks like a mutation. Treat it exactly as
 * you would treat an unauthenticated database port, because that is what it is.
 *
 * ---- WIRE PROTOCOL v1 ------------------------------------------------------
 *
 * A request is ONE line of UTF-8 terminated by \n. Requests beginning with a
 * backslash are directives; everything else is evaluated (bare qSQL included,
 * exactly as at the amber> prompt).
 *
 *   \mode text|json|jsonc|raw   set the reply encoding      -> +2\nok
 *   \parse <expr>               syntax-check, do NOT evaluate -> +2\nok
 *   \ping                       liveness                    -> +4\npong
 *   \version                    engine version              -> +5\n1.9.6
 *   \quit                       close the connection
 *   <expression>                evaluate
 *
 * A reply is a header line followed by exactly that many bytes of payload:
 *
 *   +<nbytes>\n<payload>        success
 *   -<nbytes>\n<payload>        error; the payload is the engine's own
 *                               diagnostic text
 *
 * Payload encodings:
 *
 *   text    the result rendered as the REPL renders it, ANSI stripped
 *   json    row-oriented   [{"sym":"AAPL","px":187.2}, ...]
 *   jsonc   column-oriented {"sym":["AAPL",...],"px":[187.2,...]}   <- Grafana
 *   raw     NO header line, payload only. For compatibility with the engine's
 *           own ipc.k client (`hsync`), which writes a bare query and reads
 *           until a short read. Programmatic clients should not use it: without
 *           a length there is no way to tell a complete reply from a truncated
 *           one.
 *
 * A keyed result (`select ... by sym from t`) is flattened with the grouping
 * columns in front before encoding, because neither JSON nor Arrow has a keyed
 * table and silently dropping the keys would be worse than flattening them.
 *
 * Amber - GNU AGPLv3 - see LICENSE and NOTICE.
 */
/* fileno(3) is POSIX, not ISO C. Under -std=c99 glibc defines __STRICT_ANSI__,
 * drops _DEFAULT_SOURCE and stops declaring it -- and an implicitly declared
 * `int fileno()` is not merely a warning here, it is a real bug on LP64. The
 * engine's own src/ar.c carries the same preamble for strdup(3) and explains it
 * at length; this is the short version. These must precede the FIRST include,
 * because any system header may pull in <features.h> and latch the mode. */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif
#ifndef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE
#endif
#ifndef _DARWIN_C_SOURCE
#define _DARWIN_C_SOURCE
#endif

#include "amber_capi.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#define MAX_CLIENTS   256
#define REQ_MAX       (1 << 20)      /* 1 MiB: a generous query, a bounded one */

enum { MODE_TEXT = 0, MODE_JSON, MODE_JSONC, MODE_RAW };

static const char *MODE_NAMES[] = { "text", "json", "jsonc", "raw" };

typedef struct {
    int   fd;
    int   mode;
    char *buf;          /* accumulated request bytes, not yet a full line */
    size_t len, cap;
} Client;

static Client clients[MAX_CLIENTS];
static int    nclients;
static int    opt_readonly;
static int    opt_verbose;
static volatile sig_atomic_t stopping;

static void on_signal(int sig) { (void)sig; stopping = 1; }

static void logf_(const char *fmt, ...) {
    va_list ap;
    if (!opt_verbose) return;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    fputc('\n', stderr);
}

/* ---- the K helpers this daemon evaluates through ------------------------- */
/*
 * Written out of core primitives, so amberd works against a BARE engine (no .k
 * standard library) as well as a full one. `r:` rather than `y:` inside the
 * lambdas is not a style choice: x, y and z are IMPLICIT PARAMETERS in Amber, so
 * assigning to y silently makes a monadic function dyadic and every call then
 * returns a projection instead of a result.
 */
static const char *BOOTSTRAP =
    "amberd.unkey:{k:+!x;v:+.x;+((!k),(!v))!((.k),(.v))}\n"
    "amberd.flat:{$[`m~@x;$[`M~@!x;amberd.unkey x;x];x]}\n"
    "amberd.json:{`j amberd.flat x}\n"
    "amberd.jsonc:{r:amberd.flat x;$[`M~@r;`j (!+r)!(. +r);`j r]}\n"
    /* Introspection, for the language server and anything else that needs to
     * know what is queryable. `amberd.tables` traps each lookup rather than
     * evaluating `. x` bare, because `!\`` reports a NAMESPACED global as a
     * single joined symbol (`repl.prompt`) while the global table is keyed on
     * the pair (namespace, name) -- so `. \`repl.prompt` raises 'value, and one
     * such name would otherwise abort the whole listing. */
    "amberd.tables:{x@&{.[{`M~@ . x};,x;0]}'x}\n"
    /* The parentheses around `. x` are load-bearing: `.` is DYADIC apply as
     * well as monadic value, so `cols . x` parses as "apply cols with the
     * argument list x" -- which is well-formed, returns an empty symbol vector,
     * and is silently the wrong answer. `cols(. x)` is "the columns of the value
     * of x". This cost an afternoon; it is written down so it costs no one else
     * one. */
    "amberd.cols:{$[`M~@(. x);cols(. x);0#`]}\n";

/* ---- write helpers ------------------------------------------------------- */

static int write_all(int fd, const char *p, size_t n) {
    while (n) {
        ssize_t k = write(fd, p, n);
        if (k < 0) { if (errno == EINTR) continue; return -1; }
        if (k == 0) return -1;
        p += k; n -= (size_t)k;
    }
    return 0;
}

static int reply(Client *c, int ok, const char *payload, size_t n) {
    char header[32];
    int hn;
    if (c->mode == MODE_RAW) return write_all(c->fd, payload, n);
    hn = snprintf(header, sizeof header, "%c%zu\n", ok ? '+' : '-', n);
    if (write_all(c->fd, header, (size_t)hn) != 0) return -1;
    return write_all(c->fd, payload, n);
}

static int reply_str(Client *c, int ok, const char *s) {
    return reply(c, ok, s, strlen(s));
}

/* ---- read-only guard ----------------------------------------------------- */
/*
 * A best-effort syntactic filter, and labelled as such rather than as a
 * security boundary: Amber is homoiconic, so `. "t:1"` assigns without any of
 * these tokens appearing, and no token scan can ever close that. It exists to
 * stop an ACCIDENT -- a dashboard panel with a typo, a query built by string
 * concatenation -- not an attacker. The real boundary is not exposing the port.
 */
static int isalnum_(unsigned char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

static int looks_like_mutation(const char *line) {
    static const char *TOKENS[] = { "::", "delete ", "upd\"", "del\"", "hdel", "\\\\", 0 };
    const char *scan;
    int i;
    for (i = 0; TOKENS[i]; i++) if (strstr(line, TOKENS[i])) return 1;
    /* a bare `name:` assignment at the start of the line */
    for (scan = line; *scan; scan++) {
        if (*scan == ':' && scan > line && scan[1] != ':' && scan[-1] != ':') {
            const char *back = scan - 1;
            while (back > line && (isalnum_((unsigned char)*back) || *back == '.')) back--;
            if (back == line || *back == ' ' || *back == ';') return 1;
        }
        if (*scan == '"' || *scan == '{') break;   /* stop at the first literal */
    }
    return 0;
}

/* ---- request handling ---------------------------------------------------- */

/* \parse EXPR -- syntax-check without evaluating.
 *
 * This is the primitive a language server needs and the one the engine does not
 * expose directly: everything else it could call either evaluates (side effects
 * in an editor: unacceptable) or reports nothing useful. `\ast` is the exception
 * -- it runs the real parser and NEVER touches the compiler or the evaluator
 * (see src/ast.h) -- but it communicates failure by printing "\ast: parse error"
 * to stdout rather than by returning an error value, so the only way to read its
 * verdict is to capture fd 1 for the duration. That is what this does.
 *
 * Capturing the DESCRIPTOR rather than redirecting a stream is the whole trick:
 * ast_cmd() writes with write(1,...) from C and would not notice anything done
 * to a FILE* or to a language runtime's stdout object.
 */
static int handle_parse(Client *c, const char *expr) {
    char escaped[REQ_MAX / 4];
    char *source;
    size_t n = strlen(expr), i, j = 0;
    FILE *sink;
    int saved_fd, ok;
    amber_value res;
    char captured[4096];
    size_t got;

    if (n * 2 + 32 >= sizeof escaped)
        return reply_str(c, 0, "'limit: expression too long to syntax-check");
    for (i = 0; i < n; i++) {
        if (expr[i] == '\\' || expr[i] == '"') escaped[j++] = '\\';
        escaped[j++] = expr[i];
    }
    escaped[j] = 0;

    source = (char *)malloc(j + 32);
    if (!source) return reply_str(c, 0, "'limit: out of memory");
    snprintf(source, j + 32, ". \"\\\\ast %s\"", escaped);

    sink = tmpfile();
    if (!sink) { free(source); return reply_str(c, 0, "'io: could not open a capture buffer"); }
    fflush(stdout);
    saved_fd = dup(1);
    if (saved_fd >= 0) dup2(fileno(sink), 1);
    res = amber_eval_str(source);
    fflush(stdout);
    if (saved_fd >= 0) { dup2(saved_fd, 1); close(saved_fd); }
    free(source);
    if (res) amber_release(res);

    rewind(sink);
    got = fread(captured, 1, sizeof captured - 1, sink);
    captured[got] = 0;
    fclose(sink);

    ok = strstr(captured, "parse error") == 0;
    return ok ? reply_str(c, 1, "ok")
              : reply_str(c, 0, "'parse: the expression could not be parsed");
}

static int handle_directive(Client *c, const char *line) {
    int i;
    if (!strncmp(line, "\\mode ", 6)) {
        for (i = 0; i < 4; i++)
            if (!strcmp(line + 6, MODE_NAMES[i])) {
                c->mode = i;
                return reply_str(c, 1, "ok");
            }
        return reply_str(c, 0, "'domain: unknown mode (text|json|jsonc|raw)");
    }
    if (!strcmp(line, "\\mode"))    return reply_str(c, 1, MODE_NAMES[c->mode]);
    if (!strncmp(line, "\\parse ", 7)) return handle_parse(c, line + 7);
    if (!strcmp(line, "\\ping"))    return reply_str(c, 1, "pong");
    if (!strcmp(line, "\\version")) return reply_str(c, 1, amber_version_string());
    if (!strcmp(line, "\\quit"))    return -1;
    return reply_str(c, 0, "'domain: unknown directive (\\mode \\parse \\ping \\version \\quit)");
}

static int handle_query(Client *c, const char *line) {
    amber_value result, encoded;
    char *text;
    int rc;

    if (opt_readonly && looks_like_mutation(line))
        return reply_str(c, 0, "'domain: this amberd is --readonly and the request looks like a mutation");

    result = amber_eval_qsql(line);
    if (!result) return reply_str(c, 0, amber_last_error());

    switch (c->mode) {
        case MODE_JSON:
        case MODE_JSONC: {
            amber_value args[1];
            args[0] = result;
            encoded = amber_call(c->mode == MODE_JSON ? "amberd.json" : "amberd.jsonc", args, 1);
            amber_release(result);
            if (!encoded) return reply_str(c, 0, amber_last_error());
            {
                long long need = amber_to_string(encoded, 0, 0);
                char *buf;
                if (need < 0) { amber_release(encoded); return reply_str(c, 0, "'type: encoder did not return text"); }
                buf = (char *)malloc((size_t)need + 1);
                if (!buf) { amber_release(encoded); return reply_str(c, 0, "'limit: out of memory"); }
                amber_to_string(encoded, buf, need + 1);
                rc = reply(c, 1, buf, (size_t)need);
                free(buf);
                amber_release(encoded);
            }
            return rc;
        }
        default:
            text = amber_format_plain(result);
            amber_release(result);
            if (!text) return reply_str(c, 0, amber_last_error());
            rc = reply(c, 1, text, strlen(text));
            amber_free(text);
            return rc;
    }
}

static int handle_line(Client *c, char *line) {
    size_t n = strlen(line);
    while (n && (line[n - 1] == '\r' || line[n - 1] == ' ')) line[--n] = 0;
    if (!n) return reply_str(c, 1, "");
    logf_("amberd: [%d] %s", c->fd, line);
    if (line[0] == '\\') return handle_directive(c, line);
    return handle_query(c, line);
}

/* ---- connection bookkeeping ---------------------------------------------- */

static void client_drop(int index) {
    close(clients[index].fd);
    free(clients[index].buf);
    clients[index] = clients[nclients - 1];
    nclients--;
}

static int client_feed(int index) {
    Client *c = &clients[index];
    char chunk[8192];
    ssize_t got = read(c->fd, chunk, sizeof chunk);
    char *nl;

    if (got <= 0) return -1;
    if (c->len + (size_t)got + 1 > c->cap) {
        size_t want = (c->len + (size_t)got + 1) * 2;
        char *grown;
        if (want > REQ_MAX) {
            reply_str(c, 0, "'limit: request exceeds the 1 MiB line limit");
            return -1;
        }
        grown = (char *)realloc(c->buf, want);
        if (!grown) return -1;
        c->buf = grown;
        c->cap = want;
    }
    memcpy(c->buf + c->len, chunk, (size_t)got);
    c->len += (size_t)got;
    c->buf[c->len] = 0;

    while ((nl = memchr(c->buf, '\n', c->len)) != 0) {
        size_t linelen = (size_t)(nl - c->buf);
        size_t rest = c->len - linelen - 1;
        char *line = (char *)malloc(linelen + 1);
        int rc;
        if (!line) return -1;
        memcpy(line, c->buf, linelen);
        line[linelen] = 0;
        memmove(c->buf, nl + 1, rest);
        c->len = rest;
        c->buf[c->len] = 0;
        rc = handle_line(c, line);
        free(line);
        if (rc != 0) return -1;
    }
    return 0;
}

/* ---- main ---------------------------------------------------------------- */

static void usage(void) {
    fputs(
    "amberd " "-- a TCP query server for the Amber engine\n"
    "\n"
    "usage: amberd [options]\n"
    "\n"
    "  -p, --port N          listen on port N (default 5010)\n"
    "  -b, --bind ADDR       bind address (default 127.0.0.1)\n"
    "  -H, --home DIR        load the .k standard library from DIR\n"
    "  -l, --load FILE.k     load a script after startup (repeatable)\n"
    "  -e, --eval EXPR       evaluate EXPR after startup (repeatable)\n"
    "  -m, --mode MODE       default reply encoding: text|json|jsonc|raw\n"
    "      --readonly        reject requests that look like mutations\n"
    "      --i-know-this-is-unauthenticated\n"
    "                        allow binding a non-loopback address\n"
    "  -v, --verbose         log each request to stderr\n"
    "  -h, --help            this message\n"
    "\n"
    "There is NO authentication. A client that can connect can evaluate\n"
    "arbitrary Amber. Bind loopback and put a real proxy in front of it.\n",
    stderr);
}

int main(int argc, char **argv) {
    const char *bind_addr = "127.0.0.1", *home = 0;
    int port = 5010, default_mode = MODE_TEXT, allow_public = 0;
    const char *scripts[64]; int nscripts = 0;
    const char *evals[64];   int nevals = 0;
    int listener, on = 1, i;
    struct sockaddr_in addr;
    struct pollfd fds[MAX_CLIENTS + 1];
    amber_value res;

    for (i = 1; i < argc; i++) {
        const char *a = argv[i];
        #define NEXT() (i + 1 < argc ? argv[++i] : (usage(), exit(2), ""))
        if      (!strcmp(a, "-p") || !strcmp(a, "--port")) port = atoi(NEXT());
        else if (!strcmp(a, "-b") || !strcmp(a, "--bind")) bind_addr = NEXT();
        else if (!strcmp(a, "-H") || !strcmp(a, "--home")) home = NEXT();
        else if (!strcmp(a, "-l") || !strcmp(a, "--load")) { if (nscripts < 64) scripts[nscripts++] = NEXT(); }
        else if (!strcmp(a, "-e") || !strcmp(a, "--eval")) { if (nevals < 64) evals[nevals++] = NEXT(); }
        else if (!strcmp(a, "-m") || !strcmp(a, "--mode")) {
            const char *m = NEXT(); int k; default_mode = -1;
            for (k = 0; k < 4; k++) if (!strcmp(m, MODE_NAMES[k])) default_mode = k;
            if (default_mode < 0) { fprintf(stderr, "amberd: unknown mode '%s'\n", m); return 2; }
        }
        else if (!strcmp(a, "--readonly")) opt_readonly = 1;
        else if (!strcmp(a, "--i-know-this-is-unauthenticated")) allow_public = 1;
        else if (!strcmp(a, "-v") || !strcmp(a, "--verbose")) opt_verbose = 1;
        else if (!strcmp(a, "-h") || !strcmp(a, "--help")) { usage(); return 0; }
        else { fprintf(stderr, "amberd: unknown option '%s'\n", a); usage(); return 2; }
        #undef NEXT
    }

    if (strcmp(bind_addr, "127.0.0.1") && strcmp(bind_addr, "localhost") && !allow_public) {
        fprintf(stderr,
            "amberd: refusing to bind %s.\n"
            "  This server has no authentication: anyone who can reach the port can\n"
            "  evaluate arbitrary Amber, which includes reading files and spawning\n"
            "  processes. If that is genuinely what you want, pass\n"
            "  --i-know-this-is-unauthenticated.\n", bind_addr);
        return 2;
    }

    if (amber_abi_version() != AMBER_CAPI_ABI) {
        fprintf(stderr, "amberd: libamber ABI mismatch (built for %d, loaded %d)\n",
                AMBER_CAPI_ABI, amber_abi_version());
        return 1;
    }
    if (amber_init(home) != 0) {
        fprintf(stderr, "amberd: %s\n", amber_last_error());
        return 1;
    }
    amber_set_diagnostics(0);
    res = amber_eval_str(BOOTSTRAP);
    if (!res) { fprintf(stderr, "amberd: bootstrap failed: %s\n", amber_last_error()); return 1; }
    amber_release(res);

    for (i = 0; i < nscripts; i++) {
        char expr[4096];
        snprintf(expr, sizeof expr, ".[{. `c$1:x};,\"%s\";{'x}]", scripts[i]);
        res = amber_eval_str(expr);
        if (!res) { fprintf(stderr, "amberd: loading %s: %s\n", scripts[i], amber_last_error()); return 1; }
        amber_release(res);
        fprintf(stderr, "amberd: loaded %s\n", scripts[i]);
    }
    for (i = 0; i < nevals; i++) {
        res = amber_eval_qsql(evals[i]);
        if (!res) { fprintf(stderr, "amberd: --eval %s: %s\n", evals[i], amber_last_error()); return 1; }
        amber_release(res);
    }

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    signal(SIGPIPE, SIG_IGN);          /* a client that vanishes mid-reply must
                                        * fail the write, not kill the server */

    listener = socket(AF_INET, SOCK_STREAM, 0);
    if (listener < 0) { perror("amberd: socket"); return 1; }
    setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &on, sizeof on);
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_port = htons((unsigned short)port);
    addr.sin_addr.s_addr = inet_addr(strcmp(bind_addr, "localhost") ? bind_addr : "127.0.0.1");
    if (bind(listener, (struct sockaddr *)&addr, sizeof addr) < 0) { perror("amberd: bind"); return 1; }
    if (listen(listener, 64) < 0) { perror("amberd: listen"); return 1; }

    fprintf(stderr, "amberd: Amber %s listening on %s:%d (mode %s%s%s)\n",
            amber_version_string(), bind_addr, port, MODE_NAMES[default_mode],
            opt_readonly ? ", readonly" : "",
            home ? "" : ", BARE engine -- no .k standard library, pass --home");

    while (!stopping) {
        int n = 0, ready;
        fds[n].fd = listener; fds[n].events = POLLIN; n++;
        for (i = 0; i < nclients; i++) { fds[n].fd = clients[i].fd; fds[n].events = POLLIN; n++; }

        ready = poll(fds, (nfds_t)n, 1000);
        if (ready < 0) { if (errno == EINTR) continue; perror("amberd: poll"); break; }
        if (!ready) continue;

        /* Serve clients before accepting: a burst of new connections must never
         * starve the ones already waiting for an answer. */
        for (i = nclients - 1; i >= 0; i--)
            if (fds[i + 1].revents & (POLLIN | POLLHUP | POLLERR))
                if (client_feed(i) != 0) { logf_("amberd: [%d] closed", clients[i].fd); client_drop(i); }

        if (fds[0].revents & POLLIN) {
            int fd = accept(listener, 0, 0);
            if (fd >= 0) {
                if (nclients >= MAX_CLIENTS) {
                    const char *busy = "-24\n'limit: too many clients\n";
                    write_all(fd, busy, strlen(busy));
                    close(fd);
                } else {
                    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof on);
                    clients[nclients].fd = fd;
                    clients[nclients].mode = default_mode;
                    clients[nclients].buf = 0;
                    clients[nclients].len = clients[nclients].cap = 0;
                    nclients++;
                    logf_("amberd: [%d] connected (%d open)", fd, nclients);
                }
            }
        }
    }

    fprintf(stderr, "amberd: shutting down (%d client%s)\n", nclients, nclients == 1 ? "" : "s");
    while (nclients) client_drop(nclients - 1);
    close(listener);
    amber_shutdown();
    return 0;
}
