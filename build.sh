#!/usr/bin/env bash
# build.sh  -  build libamber_arrow.so (and, with --amberd, the query daemon).
#
#   ./build.sh                       library only
#   ./build.sh --amberd              library + amberd
#   AMBER_SRC=/path/to/amber ./build.sh
#
# Both artefacts link against libamber.so and against nothing else -- no
# libarrow, no gRPC, no protobuf. The Arrow C Data Interface is a set of struct
# layouts, not a library, which is exactly why a 300-line C file can produce
# batches that PyArrow, DuckDB, Polars and arrow-rs all read natively.
set -euo pipefail
cd "$(cd "$(dirname "$0")" && pwd -P)"

WANT_AMBERD=0
for a in "$@"; do case "$a" in --amberd) WANT_AMBERD=1;; esac; done

CC="${CC:-cc}"
SOEXT=so; SOFLAGS="-shared"
if [ "$(uname -s 2>/dev/null)" = Darwin ]; then
  SOEXT=dylib; SOFLAGS="-dynamiclib -install_name @rpath/libamber_arrow.dylib"
fi

# --- locate the engine ------------------------------------------------------
AMBER_SRC="${AMBER_SRC:-../amber}"
AMBER_LIB_DIR="${AMBER_LIB_DIR:-$AMBER_SRC}"
if [ ! -f "$AMBER_LIB_DIR/libamber.$SOEXT" ]; then
  if [ -x "$AMBER_SRC/build.sh" ]; then
    echo "amber-arrow: building the engine in $AMBER_SRC"
    ( cd "$AMBER_SRC" && ./build.sh --shared )
    AMBER_LIB_DIR="$AMBER_SRC"
  else
    echo "amber-arrow: no libamber.$SOEXT found." >&2
    echo "  set AMBER_SRC to an engine checkout, or AMBER_LIB_DIR to a prebuilt library." >&2
    exit 1
  fi
fi
AMBER_LIB_DIR="$(cd "$AMBER_LIB_DIR" && pwd -P)"
echo "amber-arrow: engine at $AMBER_LIB_DIR"

F="-O2 -Wall -Wextra -Wno-unused-parameter -std=c99 -Iinclude -fPIC"
mkdir -p build

echo "amber-arrow: compiling libamber_arrow.$SOEXT"
"$CC" $F $SOFLAGS -o "build/libamber_arrow.$SOEXT" src/amber_arrow.c \
      -L"$AMBER_LIB_DIR" -lamber -Wl,-rpath,"$AMBER_LIB_DIR"
echo "amber-arrow: built build/libamber_arrow.$SOEXT"

if [ "$WANT_AMBERD" = 1 ]; then
  echo "amber-arrow: compiling amberd"
  "$CC" -O2 -Wall -Wextra -Wno-unused-parameter -std=c99 -Iinclude -o build/amberd amberd/amberd.c \
        -L"$AMBER_LIB_DIR" -lamber -Wl,-rpath,"$AMBER_LIB_DIR"
  echo "amber-arrow: built build/amberd"
fi
