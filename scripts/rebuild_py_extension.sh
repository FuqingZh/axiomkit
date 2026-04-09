#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  ./scripts/rebuild_py_extension.sh [--release] [--no-import-check]

Examples:
  ./scripts/rebuild_py_extension.sh
  ./scripts/rebuild_py_extension.sh --release

Notes:
  - Rebuilds the top-level Rust Python extension from `crates/axiomkit_py`.
  - Copies the built shared library into `python/axiomkit/` for local development.
  - This is the command to run after editing Rust code that is surfaced through Python.
EOF
}

PROFILE="debug"
RUN_IMPORT_CHECK=1

while [[ $# -gt 0 ]]; do
    case "$1" in
        --release)
            PROFILE="release"
            shift
            ;;
        --no-import-check)
            RUN_IMPORT_CHECK=0
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage
            exit 2
            ;;
    esac
done

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

case "$(uname -s)" in
    Linux)
        SRC_CANDIDATES=("target/$PROFILE/lib_axiomkit_rs.so")
        DEFAULT_DEST="python/axiomkit/_axiomkit_rs.abi3.so"
        ;;
    Darwin)
        SRC_CANDIDATES=("target/$PROFILE/lib_axiomkit_rs.dylib" "target/$PROFILE/lib_axiomkit_rs.so")
        DEFAULT_DEST="python/axiomkit/_axiomkit_rs.abi3.so"
        ;;
    MINGW*|MSYS*|CYGWIN*)
        SRC_CANDIDATES=("target/$PROFILE/_axiomkit_rs.dll" "target/$PROFILE/axiomkit_rs.dll" "target/$PROFILE/_axiomkit_rs.pyd")
        DEFAULT_DEST="python/axiomkit/_axiomkit_rs.pyd"
        ;;
    *)
        echo "Unsupported platform: $(uname -s)" >&2
        exit 1
        ;;
esac

DEST_PATH="$(find "$ROOT_DIR/python/axiomkit" -maxdepth 1 -type f \( -name '_axiomkit_rs*.so' -o -name '_axiomkit_rs*.dylib' -o -name '_axiomkit_rs*.pyd' \) | head -n 1)"
if [[ -z "$DEST_PATH" ]]; then
    DEST_PATH="$ROOT_DIR/$DEFAULT_DEST"
fi

BUILD_ARGS=(-p axiomkit_py)
if [[ "$PROFILE" == "release" ]]; then
    BUILD_ARGS+=(--release)
fi

echo "Building axiomkit_py ($PROFILE)..."
cargo build "${BUILD_ARGS[@]}"

SRC_PATH=""
for candidate in "${SRC_CANDIDATES[@]}"; do
    if [[ -f "$ROOT_DIR/$candidate" ]]; then
        SRC_PATH="$ROOT_DIR/$candidate"
        break
    fi
done

if [[ -z "$SRC_PATH" ]]; then
    echo "Cannot find built extension under target/$PROFILE." >&2
    exit 1
fi

mkdir -p "$(dirname "$DEST_PATH")"
install -m 0755 "$SRC_PATH" "$DEST_PATH"

echo "Copied:"
echo "  $SRC_PATH"
echo "  -> $DEST_PATH"

if [[ "$RUN_IMPORT_CHECK" -eq 1 ]]; then
    echo "Running Python import check..."
    PYTHONPATH=python pdm run python -c "import axiomkit._axiomkit_rs as m; print(m.__file__)"
fi

echo "Done."
