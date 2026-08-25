/* amber_arrow.c  -  ArrowArrayStream over an Amber table. Contract: include/amber_arrow.h.
 *
 * The whole file is about one idea, so it is worth stating before the code:
 *
 *   A record batch is a WINDOW, not a slice.
 *
 * The obvious way to stream a table in batches is to cut it into pieces and
 * hand out the pieces. That copies every byte, once per stream, which for a
 * 400-million-row table is the entire cost of the operation and defeats the
 * point of using Arrow at all. So this file never cuts anything. It exports the
 * table ONCE, and every batch it yields is a set of ArrowArray structs whose
 * `buffers` pointers are the parent's pointers, with `offset` and `length`
 * moved. Arrow's own format defines exactly those semantics, for primitive and
 * for utf8 layouts alike, which is why this works without a special case per
 * type.
 *
 * That leaves precisely one hard problem, and it is a lifetime problem: batch 3
 * may outlive the stream, and batches 1 and 2 may be released before batch 3 is
 * created. So the parent export cannot be owned by the stream. It is owned by a
 * reference count that the stream holds one of, and each live batch holds one
 * of, and it is torn down by whoever drops the last one -- which is the same
 * discipline the Amber engine uses internally and the same one PyArrow uses on
 * the other side of the boundary.
 *
 * Amber - GNU AGPLv3 - see LICENSE and NOTICE.
 */
#include "amber_arrow.h"
#include "arrow_c_abi.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

/* ------------------------------------------------------------------------- */
/* errors                                                                     */
/* ------------------------------------------------------------------------- */

#define AA_ERRMAX 1024
static char aa_err[AA_ERRMAX];

static int aa_fail(const char *fmt, const char *detail) {
    snprintf(aa_err, sizeof aa_err, fmt, detail ? detail : "");
    return -1;
}

/* Two K helpers this library needs and the engine does not ship as globals.
 * Installed lazily, once, into their own namespace so they cannot collide with
 * anything a user script defines. Written out of core primitives so they work on
 * a BARE engine (no .k standard library loaded) as well as a full one -- a
 * Flight server embedded in a container has no reason to carry amber.k just to
 * flatten a keyed result. */
static int aa_bootstrap(void) {
    static int done;
    amber_value res;
    if (done) return 0;
    res = amber_eval_str(
        "amberarrow.cat:{x,y}\n"
        "amberarrow.unkey:{k:+!x;v:+.x;+((!k),(!v))!((.k),(.v))}\n");
    if (!res) return aa_fail("amber-arrow: bootstrap failed: %s", amber_last_error());
    amber_release(res);
    done = 1;
    return 0;
}

int amber_arrow_abi_version(void) { return AMBER_ARROW_ABI; }
const char *amber_arrow_last_error(void) { return aa_err; }

/* ------------------------------------------------------------------------- */
/* the shared parent export                                                   */
/* ------------------------------------------------------------------------- */

/* One export of the whole table, reference-counted. The stream holds a ref; so
 * does every batch that is still alive. Whoever drops the last one releases the
 * Arrow structs (which in turn drops the Amber refcounts on the columns). */
typedef struct {
    long refs;
    struct ArrowSchema schema;
    struct ArrowArray array;
    amber_value table;          /* retained; released with the export */
} AaParent;

static void aa_parent_ref(AaParent *p) { p->refs++; }

static void aa_parent_unref(AaParent *p) {
    if (!p) return;
    if (--p->refs > 0) return;
    if (p->array.release) p->array.release(&p->array);
    if (p->schema.release) p->schema.release(&p->schema);
    if (p->table) amber_release(p->table);
    free(p);
}

/* ------------------------------------------------------------------------- */
/* schema deep copy                                                           */
/* ------------------------------------------------------------------------- */
/* get_schema() must hand the consumer an INDEPENDENT schema it can release on
 * its own timetable -- PyArrow, for one, imports the schema, releases it, and
 * then keeps pulling batches. Handing out the parent's schema struct would mean
 * the first consumer to release it destroys the stream's own copy. Re-exporting
 * the table would be simpler to write and much worse: for a table with symbol
 * columns, export materialises every symbol to utf8, so "just export again"
 * turns a metadata request into a full column materialisation. Copying ~60
 * bytes plus one strdup per column is the honest cost. */

static void aa_schema_child_release(struct ArrowSchema *s) {
    if (!s->release) return;
    free((void *)s->name);
    if (s->children) {
        int64_t i;
        for (i = 0; i < s->n_children; i++)
            if (s->children[i]) {
                if (s->children[i]->release) s->children[i]->release(s->children[i]);
                free(s->children[i]);
            }
        free(s->children);
    }
    s->release = 0;
}

static char *aa_strdup(const char *s) {
    size_t n;
    char *out;
    if (!s) return 0;
    n = strlen(s) + 1;
    out = (char *)malloc(n);
    if (out) memcpy(out, s, n);
    return out;
}

static int aa_schema_copy(const struct ArrowSchema *src, struct ArrowSchema *dst) {
    int64_t i;
    memset(dst, 0, sizeof *dst);
    /* `format` is always a static string literal in Amber's exporter (src/ar.c's
     * afmt(), plus "+s" and "u"), so it is shared rather than duplicated. If a
     * future exporter ever produces a heap-allocated format string this must
     * duplicate it too -- hence the note, rather than a silent assumption. */
    dst->format = src->format;
    dst->name = aa_strdup(src->name);
    dst->metadata = 0;
    dst->flags = src->flags;
    dst->n_children = src->n_children;
    dst->dictionary = 0;
    dst->private_data = 0;
    dst->release = aa_schema_child_release;
    if (src->n_children) {
        dst->children = (struct ArrowSchema **)calloc((size_t)src->n_children, sizeof(void *));
        if (!dst->children) return -1;
        for (i = 0; i < src->n_children; i++) {
            dst->children[i] = (struct ArrowSchema *)calloc(1, sizeof(struct ArrowSchema));
            if (!dst->children[i]) return -1;
            if (aa_schema_copy(src->children[i], dst->children[i]) != 0) return -1;
        }
    }
    return 0;
}

/* ------------------------------------------------------------------------- */
/* one batch: a window onto the parent's buffers                              */
/* ------------------------------------------------------------------------- */

typedef struct {
    AaParent *parent;
    struct ArrowArray **children;   /* the child structs this batch allocated */
    int64_t n_children;
} AaBatch;

static void aa_batch_child_release(struct ArrowArray *a) {
    if (!a->release) return;
    /* The `buffers` ARRAY is this batch's (one calloc per child); the buffers it
     * POINTS AT are the parent's and must not be touched. */
    free((void *)a->buffers);
    a->buffers = 0;
    a->release = 0;
}

static void aa_batch_release(struct ArrowArray *a) {
    AaBatch *b = (AaBatch *)a->private_data;
    int64_t i;
    if (!a->release) return;
    if (b) {
        for (i = 0; i < b->n_children; i++)
            if (b->children[i]) {
                if (b->children[i]->release) b->children[i]->release(b->children[i]);
                free(b->children[i]);
            }
        free(b->children);
        aa_parent_unref(b->parent);      /* may be the last reference */
        free(b);
    }
    free((void *)a->buffers);
    free(a->children);
    a->release = 0;
}

/* Build the window [start, start+len) over `parent` into `out`. */
static int aa_batch_make(AaParent *parent, int64_t start, int64_t len, struct ArrowArray *out) {
    const struct ArrowArray *src = &parent->array;
    AaBatch *batch;
    int64_t i, j;

    memset(out, 0, sizeof *out);
    batch = (AaBatch *)calloc(1, sizeof *batch);
    if (!batch) return aa_fail("amber-arrow: out of memory%s", 0);
    batch->parent = parent;
    batch->n_children = src->n_children;
    batch->children = (struct ArrowArray **)calloc((size_t)(src->n_children ? src->n_children : 1),
                                                   sizeof(void *));
    if (!batch->children) { free(batch); return aa_fail("amber-arrow: out of memory%s", 0); }

    /* Arm the batch's own release path NOW, before any of the allocations below
     * can fail. Every early return past this point can then just call
     * aa_batch_release(out) and be sure the partially-built structure is
     * dismantled exactly once -- calloc'd NULLs make every step of that
     * teardown a no-op on the parts that were never built. The parent ref is
     * taken here too, so the teardown's unref is always balanced. */
    aa_parent_ref(parent);
    out->private_data = batch;
    out->release = aa_batch_release;

    for (i = 0; i < src->n_children; i++) {
        const struct ArrowArray *cs = src->children[i];
        struct ArrowArray *cd = (struct ArrowArray *)calloc(1, sizeof *cd);
        if (!cd) { aa_batch_release(out); return aa_fail("amber-arrow: out of memory%s", 0); }
        batch->children[i] = cd;
        cd->length = len;
        cd->null_count = 0;
        /* THE zero-copy line: the window is expressed as an offset into the
         * parent's own buffers, exactly as Arrow's format defines it -- for a
         * primitive layout the offset counts elements, for utf8 it indexes the
         * offsets buffer. Neither case needs the bytes to move. */
        cd->offset = cs->offset + start;
        cd->n_buffers = cs->n_buffers;
        cd->n_children = 0;
        cd->buffers = (const void **)calloc((size_t)(cs->n_buffers ? cs->n_buffers : 1), sizeof(void *));
        if (!cd->buffers) { aa_batch_release(out); return aa_fail("amber-arrow: out of memory%s", 0); }
        for (j = 0; j < cs->n_buffers; j++) cd->buffers[j] = cs->buffers[j];
        cd->children = 0;
        cd->dictionary = 0;
        cd->private_data = 0;
        cd->release = aa_batch_child_release;
    }

    out->length = len;
    out->null_count = 0;
    out->offset = 0;
    out->n_buffers = 1;
    out->n_children = src->n_children;
    out->buffers = (const void **)calloc(1, sizeof(void *));
    if (!out->buffers) { aa_batch_release(out); return aa_fail("amber-arrow: out of memory%s", 0); }
    out->buffers[0] = 0;                      /* struct arrays have no validity here */
    out->children = (struct ArrowArray **)calloc((size_t)(src->n_children ? src->n_children : 1),
                                                 sizeof(void *));
    if (!out->children) { aa_batch_release(out); return aa_fail("amber-arrow: out of memory%s", 0); }
    for (i = 0; i < src->n_children; i++) out->children[i] = batch->children[i];
    out->dictionary = 0;
    return 0;
}

/* ------------------------------------------------------------------------- */
/* the stream                                                                 */
/* ------------------------------------------------------------------------- */

typedef struct {
    AaParent *parent;
    int64_t cursor;
    int64_t batch_rows;
    char err[AA_ERRMAX];
} AaStream;

static int aa_stream_get_schema(struct ArrowArrayStream *st, struct ArrowSchema *out) {
    AaStream *s = (AaStream *)st->private_data;
    if (!s) return EINVAL;
    return aa_schema_copy(&s->parent->schema, out) == 0 ? 0 : ENOMEM;
}

static int aa_stream_get_next(struct ArrowArrayStream *st, struct ArrowArray *out) {
    AaStream *s = (AaStream *)st->private_data;
    int64_t total, len;
    if (!s) return EINVAL;
    total = s->parent->array.length;
    if (s->cursor >= total) {
        /* End of stream is signalled by a released array, NOT by an error. */
        memset(out, 0, sizeof *out);
        out->release = 0;
        return 0;
    }
    len = total - s->cursor;
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
/* public entry points                                                        */
/* ------------------------------------------------------------------------- */

static AaParent *aa_parent_new(amber_value table) {
    AaParent *p = (AaParent *)calloc(1, sizeof *p);
    if (!p) { aa_fail("amber-arrow: out of memory%s", 0); return 0; }
    if (amber_arrow_export_into(table, &p->schema, &p->array) != 0) {
        aa_fail("amber-arrow: export failed: %s", amber_last_error());
        free(p);
        return 0;
    }
    p->table = amber_retain(table);
    p->refs = 1;
    return p;
}

int amber_arrow_stream_table(amber_value table, long long batch_rows, void *out_stream) {
    struct ArrowArrayStream *st = (struct ArrowArrayStream *)out_stream;
    AaParent *parent;
    AaStream *s;

    aa_err[0] = 0;
    if (aa_bootstrap() != 0) return -1;
    if (!out_stream) return aa_fail("amber-arrow: null out_stream%s", 0);
    memset(st, 0, sizeof *st);
    if (!table || !amber_is_table(table))
        return aa_fail("amber-arrow: amber_arrow_stream_table needs a table%s", 0);

    parent = aa_parent_new(table);
    if (!parent) return -1;

    s = (AaStream *)calloc(1, sizeof *s);
    if (!s) { aa_parent_unref(parent); return aa_fail("amber-arrow: out of memory%s", 0); }
    s->parent = parent;
    s->cursor = 0;
    s->batch_rows = batch_rows > 0 ? batch_rows : AMBER_ARROW_DEFAULT_BATCH;

    st->get_schema = aa_stream_get_schema;
    st->get_next = aa_stream_get_next;
    st->get_last_error = aa_stream_last_error;
    st->release = aa_stream_release;
    st->private_data = s;
    return 0;
}

int amber_arrow_stream_query(const char *query, long long batch_rows, void *out_stream) {
    amber_value result, flat;
    int rc;

    aa_err[0] = 0;
    if (aa_bootstrap() != 0) return -1;
    if (!query) return aa_fail("amber-arrow: null query%s", 0);
    result = amber_eval_qsql(query);
    if (!result) return aa_fail("amber-arrow: %s", amber_last_error());

    if (amber_is_table(result)) {
        flat = amber_retain(result);
    } else if (amber_is_dict(result)) {
        /* A keyed table -- what `select ... by sym from t` returns. Arrow has no
         * keyed-table concept, so flatten it with the grouping columns in front
         * rather than silently dropping them. */
        amber_value args[1];
        args[0] = result;
        flat = amber_call("amberarrow.unkey", args, 1);
        if (!flat) {
            amber_release(result);
            return aa_fail("amber-arrow: keyed result could not be flattened: %s",
                           amber_last_error());
        }
    } else {
        amber_release(result);
        return aa_fail("amber-arrow: query did not return a table%s", 0);
    }
    amber_release(result);
    rc = amber_arrow_stream_table(flat, batch_rows, out_stream);
    amber_release(flat);
    return rc;
}

int amber_arrow_export_table(amber_value table, void *out_schema, void *out_array) {
    aa_err[0] = 0;
    if (amber_arrow_export_into(table, out_schema, out_array) != 0)
        return aa_fail("amber-arrow: %s", amber_last_error());
    return 0;
}

amber_value amber_arrow_import_table(void *schema, void *array) {
    amber_value out;
    aa_err[0] = 0;
    out = amber_arrow_import(schema, array);
    if (!out) aa_fail("amber-arrow: %s", amber_last_error());
    return out;
}

amber_value amber_arrow_import_stream(void *stream) {
    struct ArrowArrayStream *st = (struct ArrowArrayStream *)stream;
    struct ArrowSchema schema;
    struct ArrowArray batch;
    amber_value accumulated = 0, piece;
    int rc;

    aa_err[0] = 0;
    if (aa_bootstrap() != 0) return 0;
    if (!st || !st->release) { aa_fail("amber-arrow: null or released stream%s", 0); return 0; }
    memset(&schema, 0, sizeof schema);
    rc = st->get_schema(st, &schema);
    if (rc != 0) {
        const char *why = st->get_last_error ? st->get_last_error(st) : 0;
        aa_fail("amber-arrow: get_schema failed: %s", why);
        return 0;
    }

    for (;;) {
        memset(&batch, 0, sizeof batch);
        rc = st->get_next(st, &batch);
        if (rc != 0) {
            const char *why = st->get_last_error ? st->get_last_error(st) : 0;
            aa_fail("amber-arrow: get_next failed: %s", why);
            if (accumulated) amber_release(accumulated);
            accumulated = 0;
            break;
        }
        if (!batch.release) break;               /* end of stream */

        /* Import needs its own schema: amber_arrow_import() MOVES both structs,
         * and there are many batches but only one schema. */
        {
            struct ArrowSchema per_batch;
            if (aa_schema_copy(&schema, &per_batch) != 0) {
                batch.release(&batch);
                aa_fail("amber-arrow: out of memory%s", 0);
                if (accumulated) amber_release(accumulated);
                accumulated = 0;
                break;
            }
            piece = amber_arrow_import(&per_batch, &batch);
            if (per_batch.release) per_batch.release(&per_batch);
        }
        if (batch.release) batch.release(&batch);
        if (!piece) {
            aa_fail("amber-arrow: batch import failed: %s", amber_last_error());
            if (accumulated) amber_release(accumulated);
            accumulated = 0;
            break;
        }
        if (!accumulated) {
            accumulated = piece;
        } else {
            /* `,` on two conforming tables is a row-wise concatenation. */
            amber_value args[2];
            amber_value joined;
            args[0] = accumulated;
            args[1] = piece;
            joined = amber_call("amberarrow.cat", args, 2);
            amber_release(accumulated);
            amber_release(piece);
            if (!joined) {
                aa_fail("amber-arrow: batches could not be concatenated: %s",
                        amber_last_error());
                accumulated = 0;
                break;
            }
            accumulated = joined;
        }
    }

    if (schema.release) schema.release(&schema);
    return accumulated;
}
