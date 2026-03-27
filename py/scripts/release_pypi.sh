#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  ./scripts/release_pypi.sh [--repository testpypi|pypi|<url>] [--skip-checks] [--skip-sync] [--allow-dirty]

Examples:
  ./scripts/release_pypi.sh --repository testpypi
  ./scripts/release_pypi.sh --repository pypi

Notes:
  - Default repository is testpypi.
  - Requires PDM publish credentials (env: PDM_PUBLISH_USERNAME/PASSWORD) or trusted publishing.
  - Official distributable Linux artifacts are expected to come from CI manylinux builds.
  - This script remains useful for local smoke validation and manual publishing assistance.
EOF
}

REPO="testpypi"
RUN_CHECKS=1
RUN_SYNC=1
ALLOW_DIRTY=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --repository|-r)
            REPO="${2:-}"
            shift 2
            ;;
        --skip-checks)
            RUN_CHECKS=0
            shift
            ;;
        --skip-sync)
            RUN_SYNC=0
            shift
            ;;
        --allow-dirty)
            ALLOW_DIRTY=1
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

if [[ "$ALLOW_DIRTY" -eq 0 ]] && git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    if ! git diff --quiet || ! git diff --cached --quiet; then
        echo "Refusing to publish from a dirty git tree. Commit/stash changes or use --allow-dirty." >&2
        exit 1
    fi
fi

if [[ "$RUN_SYNC" -eq 1 ]]; then
    pdm sync -G dev --no-self
fi

if [[ "$RUN_CHECKS" -eq 1 ]]; then
    pdm run ruff check src tests scripts
    pdm run pyright src
fi

if ! pdm run uv --version >/dev/null 2>&1; then
    echo "uv executable not found in the PDM environment. Run 'pdm sync -G dev --no-self' first." >&2
    exit 1
fi

rm -rf dist dist-repaired
pdm run python -m build --sdist --wheel --installer uv
if [[ "$(uname -s)" == "Linux" ]]; then
    if ! command -v patchelf >/dev/null 2>&1; then
        echo "patchelf is required for Linux wheel repair. Install it first (e.g. apt install patchelf)." >&2
        exit 1
    fi
    mkdir -p dist-repaired
    if command -v python3 >/dev/null 2>&1 && python3 -m pip --version >/dev/null 2>&1; then
        python3 -m pip install -U auditwheel
        python3 -m auditwheel repair dist/axiomkit-*.whl -w dist-repaired
    elif command -v python >/dev/null 2>&1 && python -m pip --version >/dev/null 2>&1; then
        python -m pip install -U auditwheel
        python -m auditwheel repair dist/axiomkit-*.whl -w dist-repaired
    else
        echo "python pip is unavailable, fallback to uv tool run for auditwheel." >&2
        pdm run uv tool run --from auditwheel auditwheel repair dist/axiomkit-*.whl -w dist-repaired
    fi
    rm -f dist/axiomkit-*.whl
    mv dist-repaired/*.whl dist/
fi

VERSION_RAW="$(
    pdm run python scripts/validate_wheel.py \
        --dist-dir dist \
        --require-sdist \
        --expected-manylinux-tag any \
        --print-version
)"

if [[ "$RUN_CHECKS" -eq 1 ]]; then
    pdm run python scripts/run_package_qa.py --dist-dir dist --tests-dir tests
fi

case "$REPO" in
    testpypi)
        REPO_ARG="https://test.pypi.org/legacy/"
        ;;
    pypi)
        REPO_ARG="pypi"
        ;;
    *)
        REPO_ARG="$REPO"
        ;;
esac

echo "Publishing version '$VERSION_RAW' to '$REPO_ARG'..."
pdm publish -r "$REPO_ARG" --skip-existing

echo "Publish done."
echo "If you published to TestPyPI, install via:"
echo "  pip install --index-url https://test.pypi.org/simple/ axiomkit"
