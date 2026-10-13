/* amber_arrow.c  -  Apache Arrow both ways for an Amber table. Contract: include/amber_arrow.h.
 *
 * OUT (streaming). A record batch is a WINDOW, not a slice. The table is laid
 * out ONCE as Arrow columns, and every batch is a set of ArrowArray structs
 * whose buffers are those columns' buffers with `offset` and `length` moved --
 * Arrow's own format defines exactly that, for primitives and utf8 alike, so a
 * 400-million-row result streams with no copy per batch.
 *
 * The columns are laid out here, from the engine's C API, and not by the
 * engine's own exporter, because what Arrow should SEE is the language's type,
 * not the storage's:
 *
 *   - every integer vector is a long in Amber (`type 1 2 3` is 7) however
 *     narrow it is stored, so it goes out as int64. Longs are zero-copy; the
 *     narrow ones are widened (a byte column went out as uint8, so -1 read 255);
 *   - 0N is a null (a validity bitmap over the engine's own buffer, which stays
 *     zero-copy), not -9223372036854775808;
 *   - dates, months, timestamps, timespans, times, seconds and minutes go out as
 *     date32 / timestamp[ns] / duration[ns] / time32, with the epoch moved from
 *     2000 to 1970 where Arrow's types need it (the engine refused them);
 *   - a list of strings goes out as utf8.
 *
 * Float and long columns are still the engine's own memory, pointed at.
 *
 * IN. The engine's importer reads only a few Arrow formats, ignores offsets and
 * reads anything else as 8-byte ints -- past the end of a float32 buffer. So
 * the import is done here too: every Arrow column is read (offsets, validity,
 * dictionaries, views, every int width, decimals, every temporal type) into
 * plain int64 / float64 / string columns, which become Amber vectors through the
 * C API's typed constructors, and temporal columns get their Amber type through
 * the engine's `mkd `mkp ... builtins. A stream is gathered whole and handed
 * over once, so N batches cost one engine copy, not N concatenations.
 *
 * LIFETIMES. A batch may outlive the stream, and PyArrow, DuckDB and gRPC release
 * batches from whatever thread they like. The laid-out table is refcounted
 * (atomically); the last release frees it. The engine is not thread-safe, so the
 * part of that teardown that touches the engine (dropping its refcounts) runs at
 * once only on the thread that made the export, and otherwise waits in a list
 * that the next amber_arrow_* call -- or amber_arrow_collect() -- drains.
 *
 * Amber - GNU AGPLv3 - see LICENSE and NOTICE.
 */
#include "amber_arrow.h"
#include "arrow_c_abi.h"

#include <errno.h>
#include <math.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(__GNUC__) || defined(__clang__)
#define AA_TLS __thread
#else
#define AA_TLS
#endif

#define EPOCH_DAYS 10957LL                       /* 2000.01.01 - 1970.01.01, days */
#define EPOCH_NS   946684800000000000LL          /* the same, nanoseconds */
#define DAY_MS     86400000LL

/* ------------------------------------------------------------------------- */
/* errors                                                                    */
/* ------------------------------------------------------------------------- */

#define AA_ERRMAX 1024
static AA_TLS char aa_err[AA_ERRMAX];

static int aa_fail(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(aa_err, sizeof aa_err, fmt, ap);
    va_end(ap);
    return -1;
}

int amber_arrow_abi_version(void) { return AMBER_ARROW_ABI; }
const char *amber_arrow_last_error(void) { return aa_err; }

/* K helpers, installed once into their own namespace. Core primitives only, so
 * they work on a bare engine too. */
static int aa_bootstrap(void) {
    static int done;
    amber_value res;
    if (done) return 0;
    res = amber_eval_str(
        "amberarrow.unkey:{k:+!x;v:+.x;+((!k),(!v))!((.k),(.v))}\n"
        "amberarrow.strs:{(,/x;#'x)}\n"
        "amberarrow.at:{x y}\n"
        "amberarrow.mkd:{`mkd x}\n"
        "amberarrow.mkp:{`mkp x}\n"
        "amberarrow.mkn:{`mkn x}\n"
        "amberarrow.mkt:{`mkt x}\n"
        "amberarrow.mkv:{`mkv x}\n");
    if (!res) return aa_fail("amber-arrow: bootstrap failed: %s", amber_last_error());
    amber_release(res);
    done = 1;
    return 0;
}

/* ------------------------------------------------------------------------- */
/* the laid-out table, refcounted, with deferred engine teardown              */
/* ------------------------------------------------------------------------- */

typedef struct AaParent {
    long refs;
    int64_t length, ncols;
    struct ArrowSchema schema;          /* "+s" over cschema */
    struct ArrowArray array;            /* children = carray */
    struct ArrowSchema *cschema;
    struct ArrowArray *carray;
    struct ArrowSchema **cschema_p;
    struct ArrowArray **carray_p;
    const void **bufs;                  /* 3 buffer slots per column */
    amber_value *held;                  /* engine values whose memory is pointed at */
    int64_t nheld, capheld;
    void **owned;                       /* buffers allocated here */
    int64_t nowned, capowned;
    pthread_t creator;
    struct AaParent *next_dead;
} AaParent;

static pthread_mutex_t aa_dead_mu = PTHREAD_MUTEX_INITIALIZER;
static AaParent *aa_dead;
static int aa_defer_all;

static void aa_noop_schema(struct ArrowSchema *s) { s->release = 0; }
static void aa_noop_array(struct ArrowArray *a) { a->release = 0; }

static void aa_teardown(AaParent *p) {
    int64_t i;
    if (p->cschema) for (i = 0; i < p->ncols; i++) free((void *)p->cschema[i].name);
    for (i = 0; i < p->nowned; i++) free(p->owned[i]);
    for (i = 0; i < p->nheld; i++) amber_release(p->held[i]);
    free(p->owned); free(p->held);
    free(p->cschema); free(p->carray); free(p->cschema_p); free(p->carray_p); free(p->bufs);
    free(p);
}

long amber_arrow_collect(void) {
    AaParent *list, *next;
    long n = 0;
    pthread_mutex_lock(&aa_dead_mu);
    list = aa_dead;
    aa_dead = 0;
    pthread_mutex_unlock(&aa_dead_mu);
    for (; list; list = next) { next = list->next_dead; aa_teardown(list); n++; }
    return n;
}

void amber_arrow_set_deferred(int on) { aa_defer_all = on ? 1 : 0; }

static void aa_parent_ref(AaParent *p) { __atomic_add_fetch(&p->refs, 1, __ATOMIC_RELAXED); }

static void aa_parent_unref(AaParent *p) {
    if (!p) return;
    if (__atomic_sub_fetch(&p->refs, 1, __ATOMIC_ACQ_REL) > 0) return;
    if (!aa_defer_all && pthread_equal(p->creator, pthread_self())) { aa_teardown(p); return; }
    pthread_mutex_lock(&aa_dead_mu);
    p->next_dead = aa_dead;
    aa_dead = p;
    pthread_mutex_unlock(&aa_dead_mu);
}

static void *aa_own(AaParent *p, size_t bytes) {
    void *m;
    if (p->nowned == p->capowned) {
        int64_t cap = p->capowned ? p->capowned * 2 : 16;
        void **grown = (void **)realloc(p->owned, (size_t)cap * sizeof(void *));
        if (!grown) return 0;
        p->owned = grown;
        p->capowned = cap;
    }
    m = malloc(bytes ? bytes : 1);
    if (m) p->owned[p->nowned++] = m;
    return m;
}

static int aa_hold(AaParent *p, amber_value v) {
    if (p->nheld == p->capheld) {
        int64_t cap = p->capheld ? p->capheld * 2 : 16;
        amber_value *grown = (amber_value *)realloc(p->held, (size_t)cap * sizeof(amber_value));
        if (!grown) return -1;
        p->held = grown;
        p->capheld = cap;
    }
    p->held[p->nheld++] = v;
    return 0;
}

/* ------------------------------------------------------------------------- */
/* laying out one column                                                     */
/* ------------------------------------------------------------------------- */

/* A validity bitmap from a sentinel, or NULL when there is no null at all. */
#define AA_BITMAP(NAME, T)                                                          \
static uint8_t *NAME(AaParent *p, const T *v, int64_t n, T sentinel, int64_t *nulls) { \
    int64_t i, k = 0;                                                               \
    uint8_t *bm;                                                                    \
    for (i = 0; i < n; i++) k += v[i] == sentinel;                                  \
    *nulls = k;                                                                     \
    if (!k) return 0;                                                               \
    bm = (uint8_t *)aa_own(p, (size_t)(n + 7) / 8);                                 \
    if (!bm) return 0;                                                              \
    memset(bm, 0, (size_t)(n + 7) / 8);                                             \
    for (i = 0; i < n; i++) if (v[i] != sentinel) bm[i >> 3] |= (uint8_t)(1u << (i & 7)); \
    return bm;                                                                      \
}
AA_BITMAP(aa_bitmap64, int64_t)
AA_BITMAP(aa_bitmap32, int32_t)

static int64_t aa_days_from_civil(int64_t y, unsigned m, unsigned d) {
    int64_t era;
    unsigned yoe, doy, doe;
    y -= m <= 2;
    era = (y >= 0 ? y : y - 399) / 400;
    yoe = (unsigned)(y - era * 400);
    doy = (153 * (m > 2 ? m - 3 : m + 9) + 2) / 5 + d - 1;
    doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (int64_t)doe - 719468;
}

/* Fill child i. `col` is an owned, flattened column; it is handed to p->held. */
static int aa_column(AaParent *p, int64_t i, const char *name, amber_value col) {
    struct ArrowSchema *cs = &p->cschema[i];
    struct ArrowArray *ca = &p->carray[i];
    const void **b = p->bufs + 3 * i;
    int64_t n = p->length, nulls = 0, k;
    int t = amber_type(col), vt = 0, bits = 0;
    long long len = 0;
    const void *ptr;

    if (aa_hold(p, col) != 0) { amber_release(col); return aa_fail("amber-arrow: out of memory"); }
    cs->name = (const char *)malloc(strlen(name) + 1);
    if (!cs->name) return aa_fail("amber-arrow: out of memory");
    memcpy((char *)cs->name, name, strlen(name) + 1);
    cs->flags = ARROW_FLAG_NULLABLE;
    cs->release = aa_noop_schema;
    ca->length = n;
    ca->buffers = b;
    ca->n_buffers = 2;
    ca->release = aa_noop_array;
    if (amber_count(col) != n)
        return aa_fail("amber-arrow: column `%s has %lld rows, the table %lld", name,
                       (long long)amber_count(col), (long long)n);

    ptr = (t == AMBER_T_LIST || t == AMBER_T_TABLE || t == AMBER_T_DICT) ? 0
        : amber_get_vector_ptr(col, &vt, &len, &bits);

    switch (t) {
    case AMBER_T_FLOAT:
        cs->format = "g"; b[1] = ptr;                    /* zero-copy; 0n stays NaN */
        return 0;
    case AMBER_T_LONG:
        cs->format = "l"; b[1] = ptr;                    /* zero-copy, 0N is a null */
        b[0] = aa_bitmap64(p, (const int64_t *)ptr, n, INT64_MIN, &nulls);
        ca->null_count = nulls;
        return nulls && !b[0] ? aa_fail("amber-arrow: out of memory") : 0;
    case AMBER_T_BYTE: case AMBER_T_SHORT: case AMBER_T_INT: {
        int64_t *w = (int64_t *)aa_own(p, (size_t)n * 8);
        if (!w) return aa_fail("amber-arrow: out of memory");
        for (k = 0; k < n; k++)
            w[k] = bits == 8 ? ((const int8_t *)ptr)[k] : bits == 16 ? ((const int16_t *)ptr)[k]
                 : ((const int32_t *)ptr)[k];
        cs->format = "l"; b[1] = w;
        return 0;
    }
    case AMBER_T_BOOL:
        if (bits == 1) { cs->format = "b"; b[1] = ptr; return 0; }
        {
            int64_t *w = (int64_t *)aa_own(p, (size_t)n * 8);
            if (!w) return aa_fail("amber-arrow: out of memory");
            for (k = 0; k < n; k++) w[k] = ((const uint8_t *)ptr)[k] != 0;
            cs->format = "l"; b[1] = w;
            return 0;
        }
    case AMBER_T_SYM: {
        const int32_t *ids = (const int32_t *)ptr;
        int64_t total = 0, pos = 0;
        char *data;
        for (k = 0; k < n; k++) total += (int64_t)strlen(amber_symbol_name(ids[k]));
        data = (char *)aa_own(p, (size_t)total);
        if (!data) return aa_fail("amber-arrow: out of memory");
        if (total <= INT32_MAX) {
            int32_t *off = (int32_t *)aa_own(p, (size_t)(n + 1) * 4);
            if (!off) return aa_fail("amber-arrow: out of memory");
            off[0] = 0;
            for (k = 0; k < n; k++) {
                const char *s = amber_symbol_name(ids[k]);
                size_t l = strlen(s);
                memcpy(data + pos, s, l); pos += (int64_t)l; off[k + 1] = (int32_t)pos;
            }
            cs->format = "u"; b[1] = off;
        } else {
            int64_t *off = (int64_t *)aa_own(p, (size_t)(n + 1) * 8);
            if (!off) return aa_fail("amber-arrow: out of memory");
            off[0] = 0;
            for (k = 0; k < n; k++) {
                const char *s = amber_symbol_name(ids[k]);
                size_t l = strlen(s);
                memcpy(data + pos, s, l); pos += (int64_t)l; off[k + 1] = pos;
            }
            cs->format = "U"; b[1] = off;
        }
        b[2] = data; ca->n_buffers = 3;
        return 0;
    }
    case AMBER_T_CHAR: {                                 /* a char a row */
        int32_t *off = (int32_t *)aa_own(p, (size_t)(n + 1) * 4);
        if (!off) return aa_fail("amber-arrow: out of memory");
        for (k = 0; k <= n; k++) off[k] = (int32_t)k;
        cs->format = "u"; b[1] = off; b[2] = ptr; ca->n_buffers = 3;
        return 0;
    }
    case AMBER_T_LIST: {                                 /* strings */
        amber_value pair, chars, lens, idx, args[2];
        int lt = 0, lb = 0, ct = 0, cb = 0;
        long long ln = 0, cn = 0;
        const void *lp, *cp;
        int64_t total = 0;
        if (n == 0) {
            int32_t *off = (int32_t *)aa_own(p, 4);
            if (!off) return aa_fail("amber-arrow: out of memory");
            off[0] = 0; cs->format = "u"; b[1] = off; b[2] = off; ca->n_buffers = 3;
            return 0;
        }
        args[0] = col;
        pair = amber_call("amberarrow.strs", args, 1);
        if (!pair) return aa_fail("amber-arrow: column `%s: %s", name, amber_last_error());
        if (aa_hold(p, pair) != 0) { amber_release(pair); return aa_fail("amber-arrow: out of memory"); }
        args[0] = pair; args[1] = idx = amber_int(0);
        chars = amber_call("amberarrow.at", args, 2);
        amber_release(idx);
        if (!chars || aa_hold(p, chars) != 0) {
            if (chars) amber_release(chars);
            return aa_fail("amber-arrow: column `%s: %s", name, amber_last_error());
        }
        args[1] = idx = amber_int(1);
        lens = amber_call("amberarrow.at", args, 2);
        amber_release(idx);
        if (!lens || aa_hold(p, lens) != 0) {
            if (lens) amber_release(lens);
            return aa_fail("amber-arrow: column `%s: %s", name, amber_last_error());
        }
        cp = amber_get_vector_ptr(chars, &ct, &cn, &cb);
        lp = amber_get_vector_ptr(lens, &lt, &ln, &lb);
        if (ct != AMBER_T_CHAR || !lp || ln != n || lb < 8 || lb > 64)
            return aa_fail("amber-arrow: column `%s is a general list; only a list of strings has an "
                           "Arrow layout", name);
        for (k = 0; k < n; k++)
            total += lb == 8 ? ((const int8_t *)lp)[k] : lb == 16 ? ((const int16_t *)lp)[k]
                   : lb == 32 ? ((const int32_t *)lp)[k] : ((const int64_t *)lp)[k];
        if (total != cn)
            return aa_fail("amber-arrow: column `%s is a general list; only a list of strings has an "
                           "Arrow layout", name);
        if (total <= INT32_MAX) {
            int32_t *off = (int32_t *)aa_own(p, (size_t)(n + 1) * 4);
            int64_t pos = 0;
            if (!off) return aa_fail("amber-arrow: out of memory");
            off[0] = 0;
            for (k = 0; k < n; k++) {
                pos += lb == 8 ? ((const int8_t *)lp)[k] : lb == 16 ? ((const int16_t *)lp)[k]
                     : lb == 32 ? ((const int32_t *)lp)[k] : ((const int64_t *)lp)[k];
                off[k + 1] = (int32_t)pos;
            }
            cs->format = "u"; b[1] = off;
        } else {
            int64_t *off = (int64_t *)aa_own(p, (size_t)(n + 1) * 8);
            int64_t pos = 0;
            if (!off) return aa_fail("amber-arrow: out of memory");
            off[0] = 0;
            for (k = 0; k < n; k++) { pos += ((const int64_t *)lp)[k]; off[k + 1] = pos; }
            cs->format = "U"; b[1] = off;
        }
        b[2] = cp ? cp : (const void *)b[1];
        ca->n_buffers = 3;
        return 0;
    }
    case AMBER_T_TIMESPAN_LIST: case AMBER_T_TIME_LIST: case AMBER_T_SECOND_LIST:
        /* nothing to shift: the engine's buffer, with a validity bitmap for 0N */
        cs->format = t == AMBER_T_TIMESPAN_LIST ? "tDn" : t == AMBER_T_TIME_LIST ? "ttm" : "tts";
        if ((t == AMBER_T_TIMESPAN_LIST) != (bits == 64))
            return aa_fail("amber-arrow: column `%s: unexpected %d-bit layout", name, bits);
        b[1] = ptr;
        b[0] = bits == 64 ? aa_bitmap64(p, (const int64_t *)ptr, n, INT64_MIN, &nulls)
                          : aa_bitmap32(p, (const int32_t *)ptr, n, INT32_MIN, &nulls);
        ca->null_count = nulls;
        return nulls && !b[0] ? aa_fail("amber-arrow: out of memory") : 0;
    case AMBER_T_DATE_LIST: case AMBER_T_MONTH_LIST: case AMBER_T_MINUTE_LIST: {
        const int32_t *v = (const int32_t *)ptr;
        int32_t *w = (int32_t *)aa_own(p, (size_t)n * 4);
        if (bits != 32) return aa_fail("amber-arrow: column `%s: unexpected %d-bit layout", name, bits);
        if (!w) return aa_fail("amber-arrow: out of memory");
        b[0] = aa_bitmap32(p, v, n, INT32_MIN, &nulls);
        if (nulls && !b[0]) return aa_fail("amber-arrow: out of memory");
        for (k = 0; k < n; k++) {
            int32_t x = v[k];
            if (x == INT32_MIN) { w[k] = 0; continue; }
            if (t == AMBER_T_DATE_LIST) w[k] = (int32_t)(x + EPOCH_DAYS);
            else if (t == AMBER_T_MINUTE_LIST) w[k] = x * 60;
            else {                                       /* a month: its first day */
                int64_t y = x >= 0 ? x / 12 : -((-(int64_t)x + 11) / 12);
                w[k] = (int32_t)aa_days_from_civil(2000 + y, (unsigned)(x - y * 12) + 1, 1);
            }
        }
        cs->format = t == AMBER_T_MINUTE_LIST ? "tts" : "tdD";
        b[1] = w; ca->null_count = nulls;
        return 0;
    }
    case AMBER_T_TIMESTAMP_LIST: {
        const int64_t *v = (const int64_t *)ptr;
        int64_t *w = (int64_t *)aa_own(p, (size_t)n * 8);
        if (bits != 64) return aa_fail("amber-arrow: column `%s: unexpected %d-bit layout", name, bits);
        if (!w) return aa_fail("amber-arrow: out of memory");
        b[0] = aa_bitmap64(p, v, n, INT64_MIN, &nulls);
        if (nulls && !b[0]) return aa_fail("amber-arrow: out of memory");
        for (k = 0; k < n; k++) {
            if (v[k] == INT64_MIN) { w[k] = 0; continue; }
            if (__builtin_add_overflow(v[k], EPOCH_NS, &w[k]))
                return aa_fail("amber-arrow: column `%s: a timestamp past what Arrow's nanoseconds "
                               "since 1970 can hold (year 2262)", name);
        }
        cs->format = "tsn:"; b[1] = w; ca->null_count = nulls;
        return 0;
    }
    default:
        return aa_fail("amber-arrow: column `%s is a %s, which has no Arrow layout", name,
                       amber_type_name(col));
    }
}

static AaParent *aa_parent_new(amber_value table) {
    AaParent *p;
    int64_t i, nc;
    if (!table || !amber_is_table(table)) { aa_fail("amber-arrow: not a table"); return 0; }
    nc = amber_table_ncols(table);
    p = (AaParent *)calloc(1, sizeof *p);
    if (!p) { aa_fail("amber-arrow: out of memory"); return 0; }
    p->refs = 1;
    p->creator = pthread_self();
    p->length = amber_count(table);
    p->ncols = nc;
    p->cschema = (struct ArrowSchema *)calloc((size_t)(nc ? nc : 1), sizeof *p->cschema);
    p->carray = (struct ArrowArray *)calloc((size_t)(nc ? nc : 1), sizeof *p->carray);
    p->cschema_p = (struct ArrowSchema **)calloc((size_t)(nc ? nc : 1), sizeof(void *));
    p->carray_p = (struct ArrowArray **)calloc((size_t)(nc ? nc : 1), sizeof(void *));
    p->bufs = (const void **)calloc((size_t)(nc ? nc : 1) * 3 + 1, sizeof(void *));
    if (!p->cschema || !p->carray || !p->cschema_p || !p->carray_p || !p->bufs) {
        aa_teardown(p); aa_fail("amber-arrow: out of memory"); return 0;
    }
    if (aa_hold(p, amber_retain(table)) != 0) { aa_teardown(p); aa_fail("amber-arrow: out of memory"); return 0; }
    for (i = 0; i < nc; i++) {
        amber_value col = amber_table_column(table, (int)i);
        const char *name = amber_table_colname(table, (int)i);
        p->cschema_p[i] = &p->cschema[i];
        p->carray_p[i] = &p->carray[i];
        if (!col) { aa_fail("amber-arrow: column %lld: %s", (long long)i, amber_last_error()); aa_teardown(p); return 0; }
        if (aa_column(p, i, name ? name : "", col) != 0) { aa_teardown(p); return 0; }
    }
    p->schema.format = "+s";
    p->schema.name = "";
    p->schema.n_children = nc;
    p->schema.children = p->cschema_p;
    p->schema.release = aa_noop_schema;
    p->array.length = p->length;
    p->array.n_buffers = 1;
    p->array.buffers = p->bufs + 3 * nc;               /* one NULL validity slot */
    p->array.n_children = nc;
    p->array.children = p->carray_p;
    p->array.release = aa_noop_array;
    return p;
}

/* ------------------------------------------------------------------------- */
/* schema deep copy (a consumer releases its schema on its own timetable)    */
/* ------------------------------------------------------------------------- */

static void aa_schema_release(struct ArrowSchema *s) {
    int64_t i;
    if (!s->release) return;
    free((void *)s->name);
    if (s->children) {
        for (i = 0; i < s->n_children; i++)
            if (s->children[i]) {
                if (s->children[i]->release) s->children[i]->release(s->children[i]);
                free(s->children[i]);
            }
        free(s->children);
    }
    s->release = 0;
}

static int aa_schema_copy(const struct ArrowSchema *src, struct ArrowSchema *dst) {
    int64_t i;
    size_t nl = src->name ? strlen(src->name) + 1 : 1;
    memset(dst, 0, sizeof *dst);
    dst->format = src->format;                  /* every format here is a static literal */
    dst->name = (const char *)malloc(nl);
    dst->release = aa_schema_release;
    if (!dst->name) return -1;
    memcpy((char *)dst->name, src->name ? src->name : "", nl);
    dst->flags = src->flags;
    dst->n_children = src->n_children;
    if (src->n_children) {
        dst->children = (struct ArrowSchema **)calloc((size_t)src->n_children, sizeof(void *));
        if (!dst->children) return -1;
        for (i = 0; i < src->n_children; i++) {
            dst->children[i] = (struct ArrowSchema *)calloc(1, sizeof(struct ArrowSchema));
            if (!dst->children[i] || aa_schema_copy(src->children[i], dst->children[i]) != 0) return -1;
        }
    }
    return 0;
}

/* ------------------------------------------------------------------------- */
/* one batch: a window onto the parent's buffers                             */
/* ------------------------------------------------------------------------- */

typedef struct {
    AaParent *parent;
    struct ArrowArray *children;            /* n_children structs, one calloc */
    struct ArrowArray **children_p;
    const void **bufs;                      /* 3 slots per child + 1 */
} AaBatch;

static void aa_child_release(struct ArrowArray *a) { a->release = 0; }

static void aa_batch_release(struct ArrowArray *a) {
    AaBatch *b = (AaBatch *)a->private_data;
    if (!a->release) return;
    if (b) {
        free(b->children); free(b->children_p); free(b->bufs);
        aa_parent_unref(b->parent);         /* may be the last reference */
        free(b);
    }
    a->release = 0;
}

static int aa_batch_make(AaParent *p, int64_t start, int64_t len, struct ArrowArray *out) {
    AaBatch *b;
    int64_t i, j, nc = p->ncols;
    memset(out, 0, sizeof *out);
    b = (AaBatch *)calloc(1, sizeof *b);
    if (!b) return aa_fail("amber-arrow: out of memory");
    b->children = (struct ArrowArray *)calloc((size_t)(nc ? nc : 1), sizeof *b->children);
    b->children_p = (struct ArrowArray **)calloc((size_t)(nc ? nc : 1), sizeof(void *));
    b->bufs = (const void **)calloc((size_t)nc * 3 + 1, sizeof(void *));
    if (!b->children || !b->children_p || !b->bufs) {
        free(b->children); free(b->children_p); free(b->bufs); free(b);
        return aa_fail("amber-arrow: out of memory");
    }
    b->parent = p;
    aa_parent_ref(p);
    for (i = 0; i < nc; i++) {
        const struct ArrowArray *cs = &p->carray[i];
        struct ArrowArray *cd = &b->children[i];
        cd->length = len;
        /* THE zero-copy line: the window is an offset into the parent's buffers */
        cd->offset = cs->offset + start;
        cd->null_count = cs->buffers[0] ? (start == 0 && len == p->length ? cs->null_count : -1) : 0;
        cd->n_buffers = cs->n_buffers;
        cd->buffers = b->bufs + 3 * i;
        for (j = 0; j < cs->n_buffers; j++) cd->buffers[j] = cs->buffers[j];
        cd->release = aa_child_release;
        b->children_p[i] = cd;
    }
    out->length = len;
    out->n_buffers = 1;
    out->buffers = b->bufs + 3 * nc;          /* a struct has no validity here */
    out->n_children = nc;
    out->children = b->children_p;
    out->private_data = b;
    out->release = aa_batch_release;
    return 0;
}

/* ------------------------------------------------------------------------- */
/* the stream                                                                */
/* ------------------------------------------------------------------------- */

typedef struct {
    AaParent *parent;
    int64_t cursor, batch_rows;
    char err[AA_ERRMAX];
} AaStream;

static int aa_stream_get_schema(struct ArrowArrayStream *st, struct ArrowSchema *out) {
    AaStream *s = (AaStream *)st->private_data;
    if (!s) return EINVAL;
    if (aa_schema_copy(&s->parent->schema, out) != 0) {
        if (out->release) out->release(out);
        snprintf(s->err, sizeof s->err, "amber-arrow: out of memory");
        return ENOMEM;
    }
    return 0;
}

static int aa_stream_get_next(struct ArrowArrayStream *st, struct ArrowArray *out) {
    AaStream *s = (AaStream *)st->private_data;
    int64_t len;
    if (!s) return EINVAL;
    if (s->cursor >= s->parent->length) {      /* the end is a released array, not an error */
        memset(out, 0, sizeof *out);
        return 0;
    }
    len = s->parent->length - s->cursor;
    if (len > s->batch_rows) len = s->batch_rows;
    if (aa_batch_make(s->parent, s->cursor, len, out) != 0) {
        snprintf(s->err, sizeof s->err, "%s", aa_err);
        return ENOMEM;
    }
    s->cursor += len;
    return 0;
}

static const char *aa_stream_last_error(struct ArrowArrayStream *st) {
    AaStream *s = (AaStream *)st->private_data;
    return (s && s->err[0]) ? s->err : 0;
}

static void aa_stream_release(struct ArrowArrayStream *st) {
    AaStream *s;
    if (!st->release) return;
    s = (AaStream *)st->private_data;
    if (s) { aa_parent_unref(s->parent); free(s); }
    st->private_data = 0;
    st->release = 0;
}

/* ------------------------------------------------------------------------- */
/* public: out                                                               */
/* ------------------------------------------------------------------------- */

int amber_arrow_stream_table(amber_value table, long long batch_rows, void *out_stream) {
    struct ArrowArrayStream *st = (struct ArrowArrayStream *)out_stream;
    AaParent *p;
    AaStream *s;
    aa_err[0] = 0;
    amber_arrow_collect();
    if (!out_stream) return aa_fail("amber-arrow: null out_stream");
    memset(st, 0, sizeof *st);
    if (aa_bootstrap() != 0) return -1;
    if (!table || !amber_is_table(table)) return aa_fail("amber-arrow: amber_arrow_stream_table needs a table");
    p = aa_parent_new(table);
    if (!p) return -1;
    s = (AaStream *)calloc(1, sizeof *s);
    if (!s) { aa_parent_unref(p); return aa_fail("amber-arrow: out of memory"); }
    s->parent = p;
    s->batch_rows = batch_rows > 0 ? batch_rows : AMBER_ARROW_DEFAULT_BATCH;
    st->get_schema = aa_stream_get_schema;
    st->get_next = aa_stream_get_next;
    st->get_last_error = aa_stream_last_error;
    st->release = aa_stream_release;
    st->private_data = s;
    return 0;
}

/* A query's result as a table: a keyed one flattened, keys first. Owned. */
static amber_value aa_query_table(const char *query) {
    amber_value result, flat, args[1];
    if (!query) { aa_fail("amber-arrow: null query"); return 0; }
    result = amber_eval_qsql(query);
    if (!result) { aa_fail("amber-arrow: %s", amber_last_error()); return 0; }
    if (amber_is_table(result)) return result;
    if (!amber_is_dict(result)) {
        aa_fail("amber-arrow: the query returned a %s, not a table", amber_type_name(result));
        amber_release(result);
        return 0;
    }
    args[0] = result;
    flat = amber_call("amberarrow.unkey", args, 1);
    amber_release(result);
    if (!flat) aa_fail("amber-arrow: keyed result could not be flattened: %s", amber_last_error());
    return flat;
}

int amber_arrow_stream_query(const char *query, long long batch_rows, void *out_stream) {
    amber_value t;
    int rc;
    aa_err[0] = 0;
    if (out_stream) memset(out_stream, 0, sizeof(struct ArrowArrayStream));
    if (aa_bootstrap() != 0) return -1;
    t = aa_query_table(query);
    if (!t) return -1;
    rc = amber_arrow_stream_table(t, batch_rows, out_stream);
    amber_release(t);
    return rc;
}

int amber_arrow_export_table(amber_value table, void *out_schema, void *out_array) {
    AaParent *p;
    aa_err[0] = 0;
    amber_arrow_collect();
    if (!out_schema || !out_array) return aa_fail("amber-arrow: null out_schema or out_array");
    memset(out_schema, 0, sizeof(struct ArrowSchema));
    memset(out_array, 0, sizeof(struct ArrowArray));
    if (aa_bootstrap() != 0) return -1;
    p = aa_parent_new(table);
    if (!p) return -1;
    if (aa_schema_copy(&p->schema, (struct ArrowSchema *)out_schema) != 0) {
        struct ArrowSchema *sc = (struct ArrowSchema *)out_schema;
        if (sc->release) sc->release(sc);
        aa_parent_unref(p);
        return aa_fail("amber-arrow: out of memory");
    }
    if (aa_batch_make(p, 0, p->length, (struct ArrowArray *)out_array) != 0) {
        struct ArrowSchema *sc = (struct ArrowSchema *)out_schema;
        sc->release(sc);
        aa_parent_unref(p);
        return -1;
    }
    aa_parent_unref(p);                        /* the batch holds it now */
    return 0;
}

/* ------------------------------------------------------------------------- */
/* in: reading any Arrow column into int64 / float64 / string                */
/* ------------------------------------------------------------------------- */

enum { K_LONG = 1, K_FLOAT, K_STR, K_BOOL, K_NULL };
enum { FIX_NONE = 0, FIX_MKD, FIX_MKP, FIX_MKN, FIX_MKT, FIX_MKV };
static const char *FIX_FN[] = { 0, "amberarrow.mkd", "amberarrow.mkp", "amberarrow.mkn",
                                "amberarrow.mkt", "amberarrow.mkv" };

typedef struct {                    /* how to read one Arrow format */
    int kind;                       /* K_* */
    char code;                      /* the physical layout: c C s S i I l L e f g b u U v d n */
    int fix;                        /* the Amber temporal type it becomes */
    int64_t mul, div, shift;        /* temporal: v*mul, floor /div, -shift */
    int dwidth, dscale;             /* decimal */
} AaReader;

static int aa_reader(const char *fmt, const char *col, AaReader *r) {
    memset(r, 0, sizeof *r);
    r->mul = 1; r->div = 1;
    if (!fmt || !*fmt) return aa_fail("amber-arrow: column `%s has no format", col);
    if (!fmt[1]) switch (fmt[0]) {
        case 'n': r->kind = K_NULL; r->code = 'n'; return 0;
        case 'b': r->kind = K_BOOL; r->code = 'b'; return 0;
        case 'c': case 'C': case 's': case 'S': case 'i': case 'I': case 'l': case 'L':
            r->kind = K_LONG; r->code = fmt[0]; return 0;
        case 'e': case 'f': case 'g': r->kind = K_FLOAT; r->code = fmt[0]; return 0;
        case 'u': case 'U': r->kind = K_STR; r->code = fmt[0]; return 0;
        case 'z': case 'Z':
            return aa_fail("amber-arrow: column `%s is binary; Amber has no bytes type, cast it to "
                           "a string first", col);
        default: break;
    }
    if (!strcmp(fmt, "vu")) { r->kind = K_STR; r->code = 'v'; return 0; }
    if (!strcmp(fmt, "vz"))
        return aa_fail("amber-arrow: column `%s is binary; Amber has no bytes type, cast it to "
                       "a string first", col);
    if (fmt[0] == 'd' && fmt[1] == ':') {          /* decimal: d:P,S[,W] */
        int prec = 0, scale = 0, width = 128;
        if (sscanf(fmt + 2, "%d,%d,%d", &prec, &scale, &width) < 2)
            return aa_fail("amber-arrow: column `%s: bad decimal format %s", col, fmt);
        if (width != 32 && width != 64 && width != 128)
            return aa_fail("amber-arrow: column `%s: %d-bit decimals are not supported", col, width);
        r->kind = K_FLOAT; r->code = 'd'; r->dwidth = width; r->dscale = scale;
        return 0;
    }
    if (fmt[0] == 't') {
        r->kind = K_LONG;
        if (!strcmp(fmt, "tdD")) { r->code = 'i'; r->shift = EPOCH_DAYS; r->fix = FIX_MKD; return 0; }
        if (!strcmp(fmt, "tdm")) { r->code = 'l'; r->div = DAY_MS; r->shift = EPOCH_DAYS; r->fix = FIX_MKD; return 0; }
        if (!strcmp(fmt, "tts")) { r->code = 'i'; r->fix = FIX_MKV; return 0; }
        if (!strcmp(fmt, "ttm")) { r->code = 'i'; r->fix = FIX_MKT; return 0; }
        if (!strcmp(fmt, "ttu")) { r->code = 'l'; r->mul = 1000; r->fix = FIX_MKN; return 0; }
        if (!strcmp(fmt, "ttn")) { r->code = 'l'; r->fix = FIX_MKN; return 0; }
        if ((fmt[1] == 's' && fmt[3] == ':') || fmt[1] == 'D') {
            int64_t m = fmt[2] == 's' ? 1000000000LL : fmt[2] == 'm' ? 1000000LL
                      : fmt[2] == 'u' ? 1000LL : fmt[2] == 'n' ? 1 : 0;
            if (!m || (fmt[1] == 'D' && fmt[3])) return aa_fail("amber-arrow: column `%s: unknown temporal format %s", col, fmt);
            r->code = 'l'; r->mul = m;
            if (fmt[1] == 's') { r->shift = EPOCH_NS; r->fix = FIX_MKP; }   /* any time zone: an instant, UTC */
            else r->fix = FIX_MKN;
            return 0;
        }
        return aa_fail("amber-arrow: column `%s has Arrow type %s (an interval?), which Amber has no "
                       "type for", col, fmt);
    }
    if (fmt[0] == '+')
        return aa_fail("amber-arrow: column `%s is nested (%s); Amber columns are flat", col, fmt);
    return aa_fail("amber-arrow: column `%s has Arrow format %s, which is not supported", col, fmt);
}

static double aa_half(uint16_t h) {
    int e = (h >> 10) & 31, m = h & 1023;
    double v = e == 0 ? ldexp(m, -24) : e == 31 ? (m ? NAN : INFINITY) : ldexp(m + 1024, e - 25);
    return (h & 0x8000) ? -v : v;
}

static int aa_isnull(const struct ArrowArray *a, int64_t j) {
    const uint8_t *v;
    if (a->null_count == 0 || a->n_buffers < 1 || !a->buffers[0]) return 0;
    v = (const uint8_t *)a->buffers[0];
    return !((v[j >> 3] >> (j & 7)) & 1);
}

/* Integer j (absolute) of a primitive array, as int64. *ovf for a uint64 past int64. */
/* Element j of a foreign buffer, read with memcpy: Arrow buffers that came off
 * the wire (Flight, IPC) need not be aligned, and a typed load of an unaligned
 * address is undefined behaviour (UBSan found one in a Flight upload). */
#define AA_LOAD(T, d, j) ({ T aa_v_; memcpy(&aa_v_, (const char *)(d) + (size_t)(j) * sizeof(T), sizeof(T)); aa_v_; })

static int64_t aa_int_at(char code, const void *d, int64_t j, int *ovf) {
    switch (code) {
        case 'c': return AA_LOAD(int8_t, d, j);
        case 'C': return AA_LOAD(uint8_t, d, j);
        case 's': return AA_LOAD(int16_t, d, j);
        case 'S': return AA_LOAD(uint16_t, d, j);
        case 'i': return AA_LOAD(int32_t, d, j);
        case 'I': return AA_LOAD(uint32_t, d, j);
        case 'l': return AA_LOAD(int64_t, d, j);
        case 'L': { uint64_t u = AA_LOAD(uint64_t, d, j); if (u > (uint64_t)INT64_MAX) *ovf = 1; return (int64_t)u; }
        default: return 0;
    }
}

typedef struct {                    /* one column being gathered */
    char *name;
    AaReader r, dict;               /* dict.kind != 0: the column is dictionary-encoded */
    int out;                        /* K_LONG, K_FLOAT or K_STR: what is gathered */
    int64_t n, cap;
    void *vals;                     /* int64 or double */
    int64_t *soff;                  /* strings: start of each, into chars */
    char *chars;
    int64_t clen, ccap;
} AaCol;

static int aa_grow(AaCol *c, int64_t more) {
    if (c->n + more > c->cap) {
        int64_t cap = c->cap ? c->cap : 1024;
        while (cap < c->n + more) cap *= 2;
        if (c->out == K_STR) {
            int64_t *g = (int64_t *)realloc(c->soff, (size_t)(cap + 1) * 8);
            if (!g) return -1;
            c->soff = g;
        } else {
            void *g = realloc(c->vals, (size_t)cap * 8);
            if (!g) return -1;
            c->vals = g;
        }
        c->cap = cap;
    }
    return 0;
}

static int aa_put_str(AaCol *c, const char *s, int64_t l) {
    if (c->clen + l + 1 > c->ccap) {
        int64_t cap = c->ccap ? c->ccap : 4096;
        char *g;
        while (cap < c->clen + l + 1) cap *= 2;
        g = (char *)realloc(c->chars, (size_t)cap);
        if (!g) return -1;
        c->chars = g;
        c->ccap = cap;
    }
    c->soff[c->n] = c->clen;
    if (l) memcpy(c->chars + c->clen, s, (size_t)l);
    c->chars[c->clen + l] = 0;
    c->clen += l + 1;
    c->n++;
    return 0;
}

/* The string at absolute index j of a u / U / vu array. */
static void aa_str_at(const AaReader *r, const struct ArrowArray *a, int64_t j, const char **s, int64_t *l) {
    if (r->code == 'u') {
        int32_t o0 = AA_LOAD(int32_t, a->buffers[1], j), o1 = AA_LOAD(int32_t, a->buffers[1], j + 1);
        *s = (const char *)a->buffers[2] + o0; *l = o1 - o0;
    } else if (r->code == 'U') {
        int64_t o0 = AA_LOAD(int64_t, a->buffers[1], j), o1 = AA_LOAD(int64_t, a->buffers[1], j + 1);
        *s = (const char *)a->buffers[2] + o0; *l = o1 - o0;
    } else {                                           /* a string view: 16 bytes */
        const uint8_t *v = (const uint8_t *)a->buffers[1] + 16 * j;
        int32_t len, bi, off;
        memcpy(&len, v, 4);
        *l = len;
        if (len <= 12) { *s = (const char *)v + 4; return; }
        memcpy(&bi, v + 8, 4); memcpy(&off, v + 12, 4);
        *s = (const char *)a->buffers[2 + bi] + off;
    }
}

/* Element j (absolute) of array a, read by r, appended to c. */
static int aa_put(AaCol *c, const AaReader *r, const struct ArrowArray *a, int64_t j) {
    if (r->kind == K_STR) {
        const char *s; int64_t l;
        if (aa_isnull(a, j)) return aa_put_str(c, "", 0);
        aa_str_at(r, a, j, &s, &l);
        return aa_put_str(c, s, l);
    }
    if (r->kind == K_FLOAT || r->kind == K_NULL) {
        double v = NAN;
        if (r->kind != K_NULL && !aa_isnull(a, j)) {
            const void *d = a->buffers[1];
            if (r->code == 'g') v = AA_LOAD(double, d, j);
            else if (r->code == 'f') v = AA_LOAD(float, d, j);
            else if (r->code == 'e') v = aa_half(AA_LOAD(uint16_t, d, j));
            else {                                     /* decimal */
                double m;
                if (r->dwidth == 32) m = AA_LOAD(int32_t, d, j);
                else if (r->dwidth == 64) m = (double)AA_LOAD(int64_t, d, j);
                else {
                    /* two's complement over two words: negate to a magnitude first,
                     * or hi*2^64 + lo of a small negative number rounds away to 0 */
                    uint64_t lo, uhi; int64_t hi; int neg;
                    memcpy(&lo, (const uint8_t *)d + 16 * j, 8);
                    memcpy(&hi, (const uint8_t *)d + 16 * j + 8, 8);
                    neg = hi < 0;
                    uhi = (uint64_t)hi;
                    if (neg) { lo = ~lo + 1; uhi = ~uhi + (lo == 0); }
                    m = (double)uhi * 18446744073709551616.0 + (double)lo;
                    if (neg) m = -m;
                }
                v = m / pow(10.0, r->dscale);
            }
        }
        if (c->out == K_STR) return aa_put_str(c, "", 0);
        if (c->out == K_LONG) ((int64_t *)c->vals)[c->n++] = INT64_MIN;
        else ((double *)c->vals)[c->n++] = v;
        return 0;
    }
    {                                                  /* K_LONG, K_BOOL */
        int64_t v = INT64_MIN;
        if (!aa_isnull(a, j)) {
            int ovf = 0;
            if (r->kind == K_BOOL) {
                const uint8_t *bits = (const uint8_t *)a->buffers[1];
                v = (bits[j >> 3] >> (j & 7)) & 1;
            } else {
                v = aa_int_at(r->code, a->buffers[1], j, &ovf);
                if (ovf) return aa_fail("amber-arrow: column `%s holds a uint64 past the int64 range", c->name);
                if (r->mul != 1 && __builtin_mul_overflow(v, r->mul, &v))
                    return aa_fail("amber-arrow: column `%s: a value too large for nanoseconds", c->name);
                if (r->div != 1) v = v >= 0 ? v / r->div : -((-v + r->div - 1) / r->div);
                if (r->shift && __builtin_sub_overflow(v, r->shift, &v))
                    return aa_fail("amber-arrow: column `%s: a value out of Amber's range", c->name);
            }
        }
        ((int64_t *)c->vals)[c->n++] = v;
        return 0;
    }
}

/* Append rows [0, len) of child array a (its own offset plus `po`, the parent's). */
static int aa_append(AaCol *c, const struct ArrowSchema *cs, const struct ArrowArray *a, int64_t po, int64_t len) {
    int64_t k, base = a->offset + po;
    if (aa_grow(c, len) != 0) return aa_fail("amber-arrow: out of memory");
    if (c->dict.kind) {                                /* dictionary: indices, then values */
        const struct ArrowArray *dv = a->dictionary;
        if (!dv) return aa_fail("amber-arrow: column `%s: a dictionary array without its dictionary", c->name);
        for (k = 0; k < len; k++) {
            int64_t j = base + k, idx;
            int ovf = 0;
            if (aa_isnull(a, j)) {
                if (c->out == K_STR) { if (aa_put_str(c, "", 0) != 0) return aa_fail("amber-arrow: out of memory"); }
                else if (c->out == K_FLOAT) ((double *)c->vals)[c->n++] = NAN;
                else ((int64_t *)c->vals)[c->n++] = INT64_MIN;
                continue;
            }
            idx = aa_int_at(c->r.code, a->buffers[1], j, &ovf);
            if (ovf || idx < 0 || idx >= dv->length)
                return aa_fail("amber-arrow: column `%s: a dictionary index out of range", c->name);
            if (aa_put(c, &c->dict, dv, dv->offset + idx) != 0)
                return aa_err[0] ? -1 : aa_fail("amber-arrow: out of memory");
        }
        return 0;
    }
    (void)cs;
    /* the common case, one memcpy: int64 / float64 with no nulls and no conversion */
    if ((c->r.code == 'l' || c->r.code == 'g') && c->r.mul == 1 && c->r.div == 1 && !c->r.shift &&
        (a->null_count == 0 || !a->buffers[0])) {
        memcpy((int64_t *)c->vals + c->n, (const int64_t *)a->buffers[1] + base, (size_t)len * 8);
        c->n += len;
        return 0;
    }
    for (k = 0; k < len; k++)
        if (aa_put(c, &c->r, a, base + k) != 0) return aa_err[0] ? -1 : aa_fail("amber-arrow: out of memory");
    return 0;
}

typedef struct {
    int64_t ncols;
    AaCol *cols;
} AaGather;

static void aa_gather_free(AaGather *g) {
    int64_t i;
    if (!g->cols) return;
    for (i = 0; i < g->ncols; i++) {
        free(g->cols[i].name); free(g->cols[i].vals); free(g->cols[i].soff); free(g->cols[i].chars);
    }
    free(g->cols);
    g->cols = 0;
}

static int aa_gather_init(AaGather *g, const struct ArrowSchema *sc) {
    int64_t i, j;
    memset(g, 0, sizeof *g);
    if (!sc || !sc->format || strcmp(sc->format, "+s"))
        return aa_fail("amber-arrow: a table is a struct (+s) schema, this one is %s",
                       sc && sc->format ? sc->format : "(none)");
    g->ncols = sc->n_children;
    g->cols = (AaCol *)calloc((size_t)(g->ncols ? g->ncols : 1), sizeof(AaCol));
    if (!g->cols) return aa_fail("amber-arrow: out of memory");
    for (i = 0; i < g->ncols; i++) {
        const struct ArrowSchema *cs = sc->children[i];
        const char *nm = cs->name ? cs->name : "";
        AaCol *c = &g->cols[i];
        c->name = (char *)malloc(strlen(nm) + 1);
        if (!c->name) return aa_fail("amber-arrow: out of memory");
        strcpy(c->name, nm);
        for (j = 0; j < i; j++)
            if (!strcmp(g->cols[j].name, nm)) return aa_fail("amber-arrow: two columns are called `%s", nm);
        if (cs->dictionary) {
            if (aa_reader(cs->format, nm, &c->r) != 0) return -1;
            if (c->r.kind != K_LONG || c->r.fix || c->r.mul != 1)
                return aa_fail("amber-arrow: column `%s: a dictionary index must be an integer", nm);
            if (aa_reader(cs->dictionary->format, nm, &c->dict) != 0) return -1;
        } else {
            if (aa_reader(cs->format, nm, &c->r) != 0) return -1;
        }
        {
            int k = c->dict.kind ? c->dict.kind : c->r.kind;
            c->out = k == K_STR ? K_STR : (k == K_FLOAT || k == K_NULL) ? K_FLOAT : K_LONG;
        }
    }
    return 0;
}

static int aa_gather_batch(AaGather *g, const struct ArrowSchema *sc, const struct ArrowArray *ar) {
    int64_t i;
    if (ar->n_children != g->ncols)
        return aa_fail("amber-arrow: a batch has %lld columns, its schema %lld",
                       (long long)ar->n_children, (long long)g->ncols);
    for (i = 0; i < g->ncols; i++) {
        if (ar->children[i]->length < ar->offset + ar->length)
            return aa_fail("amber-arrow: column `%s is shorter than its batch", g->cols[i].name);
        if (aa_append(&g->cols[i], sc->children[i], ar->children[i], ar->offset, ar->length) != 0) return -1;
    }
    return 0;
}

/* Hand the gathered columns to the engine. Owned table, or 0. */
static amber_value aa_gather_finish(AaGather *g) {
    amber_value *vals = 0, table = 0;
    const char **names = 0;
    int64_t i, k;
    int ok = 1;
    vals = (amber_value *)calloc((size_t)(g->ncols ? g->ncols : 1), sizeof(amber_value));
    names = (const char **)calloc((size_t)(g->ncols ? g->ncols : 1), sizeof(char *));
    if (!vals || !names) { aa_fail("amber-arrow: out of memory"); ok = 0; }
    for (i = 0; ok && i < g->ncols; i++) {
        AaCol *c = &g->cols[i];
        int kind = c->out;
        int fix = c->dict.kind ? c->dict.fix : c->r.fix;
        long long dummy = 0;
        names[i] = c->name;
        if (kind == K_STR) {
            const char **ptrs = (const char **)malloc((size_t)(c->n ? c->n : 1) * sizeof(char *));
            if (!ptrs) { aa_fail("amber-arrow: out of memory"); ok = 0; break; }
            for (k = 0; k < c->n; k++) ptrs[k] = c->chars + c->soff[k];
            vals[i] = amber_from_symbols(ptrs, c->n);
            free(ptrs);
        } else if (kind == K_FLOAT) {
            vals[i] = amber_from_float64(c->n ? (const double *)c->vals : (const double *)&dummy, c->n);
        } else {
            vals[i] = amber_from_int64(c->n ? (const long long *)c->vals : &dummy, c->n);
        }
        free(c->vals); c->vals = 0; free(c->chars); c->chars = 0; free(c->soff); c->soff = 0;
        if (!vals[i]) { aa_fail("amber-arrow: column `%s: %s", c->name, amber_last_error()); ok = 0; break; }
        if (fix) {
            amber_value typed = amber_call(FIX_FN[fix], &vals[i], 1);
            if (!typed) { aa_fail("amber-arrow: column `%s: %s", c->name, amber_last_error()); ok = 0; break; }
            amber_release(vals[i]);
            vals[i] = typed;
        }
    }
    if (ok) {
        table = amber_make_table(names, vals, (int)g->ncols);
        if (!table) aa_fail("amber-arrow: %s", amber_last_error());
    }
    if (vals) for (i = 0; i < g->ncols; i++) if (vals[i]) amber_release(vals[i]);
    free(vals);
    free(names);
    return table;
}

/* ------------------------------------------------------------------------- */
/* public: in                                                                */
/* ------------------------------------------------------------------------- */

amber_value amber_arrow_import_table(void *schema, void *array) {
    struct ArrowSchema *sc = (struct ArrowSchema *)schema;
    struct ArrowArray *ar = (struct ArrowArray *)array;
    AaGather g;
    amber_value out = 0;
    aa_err[0] = 0;
    amber_arrow_collect();
    if (!sc || !ar || !sc->release || !ar->release) {
        aa_fail("amber-arrow: null or already released Arrow struct");
        return 0;
    }
    if (aa_bootstrap() == 0 && aa_gather_init(&g, sc) == 0 && aa_gather_batch(&g, sc, ar) == 0)
        out = aa_gather_finish(&g);
    aa_gather_free(&g);
    /* the two structs are moved in: released here whatever happened */
    if (ar->release) ar->release(ar);
    if (sc->release) sc->release(sc);
    return out;
}

amber_value amber_arrow_import_stream(void *stream) {
    struct ArrowArrayStream *st = (struct ArrowArrayStream *)stream;
    struct ArrowSchema schema;
    struct ArrowArray batch;
    AaGather g;
    amber_value out = 0;
    int rc, ok;
    aa_err[0] = 0;
    amber_arrow_collect();
    memset(&g, 0, sizeof g);
    if (!st || !st->release) { aa_fail("amber-arrow: null or released stream"); return 0; }
    if (aa_bootstrap() != 0) return 0;
    memset(&schema, 0, sizeof schema);
    rc = st->get_schema(st, &schema);
    if (rc != 0) {
        const char *why = st->get_last_error ? st->get_last_error(st) : 0;
        aa_fail("amber-arrow: get_schema failed: %s", why ? why : strerror(rc));
        return 0;
    }
    ok = aa_gather_init(&g, &schema) == 0;
    while (ok) {
        memset(&batch, 0, sizeof batch);
        rc = st->get_next(st, &batch);
        if (rc != 0) {
            const char *why = st->get_last_error ? st->get_last_error(st) : 0;
            aa_fail("amber-arrow: get_next failed: %s", why ? why : strerror(rc));
            ok = 0;
            break;
        }
        if (!batch.release) break;                     /* end of stream */
        ok = aa_gather_batch(&g, &schema, &batch) == 0;
        batch.release(&batch);
    }
    if (ok) out = aa_gather_finish(&g);
    aa_gather_free(&g);
    if (schema.release) schema.release(&schema);
    return out;
}
