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

VERSION_RAW="$(awk -F'"' '/^version = /{print $2; exit}' pyproject.toml)"
if [[ -z "$VERSION_RAW" ]]; then
    echo "Cannot read project.version from pyproject.toml" >&2
    exit 1
fi

VERSION_CANONICAL="$(python -c 'import sys; from packaging.version import Version; print(Version(sys.argv[1]))' "$VERSION_RAW" 2>/dev/null || true)"
if [[ -n "$VERSION_CANONICAL" && "$VERSION_CANONICAL" != "$VERSION_RAW" ]]; then
    echo "Version canonicalization note: '$VERSION_RAW' -> '$VERSION_CANONICAL' (PEP 440)."
fi

if [[ "$RUN_SYNC" -eq 1 ]]; then
    pdm sync -G dev --no-self
fi

if [[ "$RUN_CHECKS" -eq 1 ]]; then
    pdm run ruff check src tests
    pdm run pytest -q tests
    pdm run pyright src
fi

rm -rf dist
pdm build

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
