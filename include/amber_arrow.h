/* amber_arrow.h  -  streaming Apache Arrow over the Amber engine.
 *
 * `libamber_arrow.so` sits on top of `libamber.so` and adds the one piece of
 * the Arrow C Data Interface the engine deliberately does not carry: the
 * ArrowArrayStream.
 *
 * WHY THIS IS A SEPARATE LIBRARY. The engine exports a single ArrowSchema /
 * ArrowArray pair for a whole table (src/ar.c, reached through
 * amber_arrow_export()). That is exactly the right amount of Arrow for a
 * 10,000-line C runtime to own: it is ~90 lines, it has no Arrow dependency,
 * and it covers the "hand me this table" case completely. What it does not
 * cover is the "hand me this 400-million-row table in batches, and stop when I
 * stop asking" case -- which is what an Arrow Flight server, a Polars scan and
 * any consumer with a memory budget actually need. That machinery belongs out
 * here, where it can grow, and not inside the engine, where it would be the
 * largest single feature in the tree and would be paid for by every user who
 * never touches Arrow.
 *
 * WHAT MAKES IT ZERO-COPY. Batching normally means slicing, and slicing normally
 * means copying. It does not here, because Arrow's ArrowArray carries an
 * `offset` field: a batch is produced by aliasing the parent export's buffer
 * pointers and moving `offset` and `length`. Every batch this library yields
 * points into the same Amber column payloads the engine allocated, however many
 * batches you pull. The parent export is kept alive by a reference count that
 * each live batch holds, so a consumer may hold batch 3 after having released
 * batches 1, 2 and the stream itself, and the memory it is reading stays valid.
 *
 * OWNERSHIP. Every out-parameter below is a struct the CALLER allocates. This
 * library moves into it, in the Arrow sense; the caller frees the container
 * (or lets it go out of scope) and calls `release` exactly once on the contents.
 *
 * Amber - GNU AGPLv3 - see LICENSE and NOTICE.  Arrow ABI - Apache-2.0.
 */
#ifndef AMBER_ARROW_H
#define AMBER_ARROW_H

#include "amber_capi.h"

#ifdef __cplusplus
extern "C" {
#endif

#define AMBER_ARROW_ABI 2

/* The batch size used when a caller passes 0. 65,536 rows is the figure the
 * Arrow ecosystem has converged on for streaming: large enough that per-batch
 * overhead disappears, small enough that a consumer's working set stays inside
 * L2/L3 for a handful of columns. */
#define AMBER_ARROW_DEFAULT_BATCH 65536

int amber_arrow_abi_version(void);
const char *amber_arrow_last_error(void);

/* Stream an Amber table as Arrow record batches of at most `batch_rows` rows
 * (0 selects AMBER_ARROW_DEFAULT_BATCH).
 *
 * `out_stream` must point at a caller-allocated `struct ArrowArrayStream`.
 * The table is retained for the life of the stream and of every batch it has
 * yielded. Returns 0 on success, -1 on error. */
int amber_arrow_stream_table(amber_value table, long long batch_rows, void *out_stream);

/* Evaluate `query` -- bare qSQL is accepted, as at the amber> prompt -- and
 * stream the resulting table. A keyed result (`select ... by ...`) is flattened
 * to an ordinary table with the grouping columns in front, because Arrow has no
 * notion of a keyed table and dropping the keys would be worse. */
int amber_arrow_stream_query(const char *query, long long batch_rows, void *out_stream);

/* One-shot export, for callers that want the whole table at once: fills two
 * caller-allocated structs, laid out exactly as the stream's batches are (ints
 * as int64, 0N as null, temporal columns as Arrow's temporal types). */
int amber_arrow_export_table(amber_value table, void *out_schema, void *out_array);

/* Import an Arrow schema/array pair as an Amber table (copies: Amber keeps a
 * header in front of every vector, so it cannot adopt foreign memory). Both
 * structs are MOVED in: they are released here, on success and on error.
 * Any width of int or float, bools, strings (utf8, large, views), dictionaries,
 * decimals, dates, timestamps, durations and times; offsets and nulls honoured.
 * Returns an OWNED amber_value, or 0 on error (amber_arrow_last_error()). */
amber_value amber_arrow_import_table(void *schema, void *array);

/* Drain an ArrowArrayStream produced by ANY Arrow implementation into a single
 * Amber table, concatenating its batches. This is the inbound counterpart of
 * amber_arrow_stream_table() and is what lets `amberd` accept an Arrow upload.
 * Returns an OWNED amber_value, or 0 on error. */
amber_value amber_arrow_import_stream(void *stream);

/* Batches can be released on any thread, and the engine is not thread-safe, so
 * when the last batch of an export is released on a thread other than the one
 * that made it, giving its memory back to the engine waits here. Every
 * amber_arrow_* call drains the wait list first; this drains it on demand and
 * returns how many exports it freed. Call it from the engine's thread. */
long amber_arrow_collect(void);

/* 1: always wait for amber_arrow_collect(), even on the exporting thread. For
 * hosts like Python, where "the engine's thread" is whoever holds a lock (the
 * GIL) rather than a fixed thread. */
void amber_arrow_set_deferred(int on);

#ifdef __cplusplus
}
#endif

#endif /* AMBER_ARROW_H */
