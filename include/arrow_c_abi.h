/* arrow_c_abi.h  -  the Apache Arrow C Data Interface, verbatim.
 *
 * Reproduced from arrow/cpp/src/arrow/c/abi.h under the Apache License 2.0.
 * These three struct layouts are the ABI: they are frozen by the Arrow project
 * and any two libraries that agree on them can pass columnar data between
 * themselves with no shared build, no shared allocator, and no copy.
 *
 * Copying them into this repository is the correct thing to do, not a shortcut.
 * The whole point of a C data interface is that a producer does not link
 * libarrow -- neither the Amber engine nor this library does, and neither has an
 * Arrow build dependency of any kind. The consumer (PyArrow, DuckDB, Polars,
 * arrow-rs, ...) supplies its own copy of the same declarations.
 *
 * Spec: https://arrow.apache.org/docs/format/CDataInterface.html
 */
#ifndef AMBER_ARROW_C_ABI_H
#define AMBER_ARROW_C_ABI_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE           2
#define ARROW_FLAG_MAP_KEYS_SORTED    4

struct ArrowSchema {
    const char *format;
    const char *name;
    const char *metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema **children;
    struct ArrowSchema *dictionary;
    void (*release)(struct ArrowSchema *);
    void *private_data;
};

struct ArrowArray {
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void **buffers;
    struct ArrowArray **children;
    struct ArrowArray *dictionary;
    void (*release)(struct ArrowArray *);
    void *private_data;
};

/* The streaming form. This is the one the Amber engine itself does NOT provide
 * -- src/ar.c exports a single schema/array pair -- and it is the reason this
 * library exists: a stream lets a consumer pull a large result in bounded
 * batches instead of materialising the whole thing, which is what an Arrow
 * Flight server, a Polars scan, or any pipeline with a memory budget actually
 * needs. */
struct ArrowArrayStream {
    int (*get_schema)(struct ArrowArrayStream *, struct ArrowSchema *out);
    int (*get_next)(struct ArrowArrayStream *, struct ArrowArray *out);
    const char *(*get_last_error)(struct ArrowArrayStream *);
    void (*release)(struct ArrowArrayStream *);
    void *private_data;
};

#ifdef __cplusplus
}
#endif

#endif /* AMBER_ARROW_C_ABI_H */
