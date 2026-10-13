/* amber_capi.h  -  the Amber dynamic C API, as consumed by python-amber.
 *
 * This is a STANDALONE mirror of section 6 of the core engine's src/ext.h. It
 * is vendored here on purpose, and the purpose is the whole architecture:
 * python-amber is a separate repository that links against libamber.so at the
 * ABI level, so it must be buildable from a checkout of ITSELF plus a shared
 * library -- never from a checkout of the engine's private headers. Nothing
 * here includes, or needs, anything from the engine's src/.
 *
 * Drift between this file and the engine is caught at RUNTIME, not hoped away:
 * amber_abi_version() is checked once during module import against
 * AMBER_CAPI_ABI below, and a mismatch is a hard ImportError naming both
 * numbers. Adding an entry point does not change the ABI number; changing the
 * shape of an existing one does.
 *
 * Ownership, restated because it is the only rule that matters here:
 *   - a function that RETURNS an amber_value returns one you own; release it
 *     exactly once with amber_release()
 *   - a function that TAKES an amber_value borrows it
 *   - amber_release() on a tagged immediate is a documented no-op
 *
 * Amber - GNU AGPLv3 - see LICENSE and NOTICE.
 */
#ifndef AMBER_CAPI_H
#define AMBER_CAPI_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define AMBER_CAPI_ABI 1

typedef unsigned long long amber_value;

enum {
    AMBER_T_LIST       = 1,
    AMBER_T_RANGE      = 2,
    AMBER_T_BOOL       = 3,
    AMBER_T_BYTE       = 4,
    AMBER_T_SHORT      = 5,
    AMBER_T_INT        = 6,
    AMBER_T_LONG       = 7,
    AMBER_T_FLOAT      = 8,
    AMBER_T_CHAR       = 9,
    AMBER_T_SYM        = 10,
    AMBER_T_TABLE      = 11,
    AMBER_T_DICT       = 12,
    AMBER_T_INT_ATOM   = 13,
    AMBER_T_LONG_ATOM  = 14,
    AMBER_T_FLOAT_ATOM = 15,
    AMBER_T_CHAR_ATOM  = 16,
    AMBER_T_SYM_ATOM   = 17,
    AMBER_T_DATE       = 26,
    AMBER_T_TIME       = 27,
    AMBER_T_TIMESTAMP  = 28,
    AMBER_T_MINUTE     = 29,   /* minutes of day (2.9)              */
    AMBER_T_SECOND     = 30,   /* seconds of day (2.9)              */
    AMBER_T_MONTH      = 31,   /* months since 2000.01 (2.9)        */
    AMBER_T_TIMESPAN   = 32,   /* nanoseconds (2.9)                 */
    /* 2.9: a list of one temporal kind is its own vector type, held as its numbers
     * (int32, or int64 for timestamps and timespans) */
    AMBER_T_DATE_LIST      = 33,
    AMBER_T_TIME_LIST      = 34,
    AMBER_T_TIMESTAMP_LIST = 35,
    AMBER_T_MINUTE_LIST    = 36,
    AMBER_T_SECOND_LIST    = 37,
    AMBER_T_MONTH_LIST     = 38,
    AMBER_T_TIMESPAN_LIST  = 39
};

/* identity */
int         amber_abi_version(void);
const char *amber_version_string(void);

/* lifecycle */
int  amber_init(const char *home);
void amber_shutdown(void);

/* evaluation */
amber_value amber_eval_str(const char *src);
amber_value amber_eval_qsql(const char *line);
amber_value amber_call(const char *fname, const amber_value *args, int argc);
const char *amber_last_error(void);
int         amber_set_diagnostics(int on);

/* ownership */
amber_value amber_retain(amber_value handle);
void        amber_release(amber_value handle);

/* introspection */
int         amber_type(amber_value handle);
const char *amber_type_name(amber_value handle);
long long   amber_count(amber_value handle);
int         amber_is_table(amber_value handle);
int         amber_is_dict(amber_value handle);
int         amber_is_atom(amber_value handle);

/* flat vectors */
const void *amber_get_vector_ptr(amber_value handle, int *out_type,
                                 long long *out_len, int *out_elem_bits);
amber_value amber_flatten(amber_value handle);
const char *amber_symbol_name(int symbol_id);

/* scalars */
long long amber_to_int(amber_value handle, int *ok);
double    amber_to_float(amber_value handle, int *ok);
int       amber_to_symbol_id(amber_value handle, int *ok);
long long amber_to_string(amber_value handle, char *dst, long long cap);

/* tables and dictionaries */
int         amber_table_ncols(amber_value table);
const char *amber_table_colname(amber_value table, int col);
amber_value amber_table_column(amber_value table, int col);
amber_value amber_dict_keys(amber_value dict);
amber_value amber_dict_values(amber_value dict);

/* construction */
amber_value amber_int(long long value);
amber_value amber_float(double value);
amber_value amber_symbol(const char *name);
amber_value amber_null(void);
amber_value amber_make_list(const amber_value *items, long long n);
amber_value amber_from_int32(const int *src, long long n);
amber_value amber_from_int64(const long long *src, long long n);
amber_value amber_from_float64(const double *src, long long n);
amber_value amber_from_bytes(const char *src, long long n);
amber_value amber_from_symbols(const char *const *src, long long n);
amber_value amber_make_table(const char *const *names, const amber_value *cols, int ncols);
amber_value amber_make_dict(const char *const *keys, const amber_value *vals, int n);
int         amber_set_global(const char *name, amber_value v);
amber_value amber_get_global(const char *name);

/* Apache Arrow C Data Interface (opaque; the caller supplies its own structs) */
int         amber_arrow_export(amber_value table, void **out_schema, void **out_array);
int         amber_arrow_export_into(amber_value table, void *out_schema, void *out_array);
amber_value amber_arrow_import(void *schema, void *array);

/* rendering */
char *amber_format(amber_value handle);
char *amber_format_plain(amber_value handle);
void  amber_free(void *p);

/* plugins */
int amber_plugin_load(const char *path);

#ifdef __cplusplus
}
#endif

#endif /* AMBER_CAPI_H */
