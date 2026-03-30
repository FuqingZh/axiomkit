from __future__ import annotations

import argparse
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
PATH_CARGO_TOML = ROOT / "crates" / "axiomkit_py" / "Cargo.toml"
PATH_CARGO_LOCK = ROOT / "Cargo.lock"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync the release version into Cargo manifest and lockfile."
    )
    parser.add_argument(
        "--version",
        required=True,
        help="Release version to write (typically the publish tag).",
    )
    return parser.parse_args()


def replace_once(text: str, pattern: str, replacement: str, context: str) -> str:
    updated, count = re.subn(pattern, replacement, text, count=1, flags=re.MULTILINE)
    if count != 1:
        raise RuntimeError(f"Expected to update exactly one {context}, got {count}")
    return updated


def update_cargo_toml(version: str) -> None:
    text = PATH_CARGO_TOML.read_text()
    updated = replace_once(
        text,
        r'(^name = "axiomkit_py"\nversion = ")[^"]+(")',
        rf"\g<1>{version}\2",
        "axiomkit_py Cargo.toml version",
    )
    PATH_CARGO_TOML.write_text(updated)


def update_cargo_lock(version: str) -> None:
    text = PATH_CARGO_LOCK.read_text()
    updated = replace_once(
        text,
        r'(\[\[package\]\]\nname = "axiomkit_py"\nversion = ")[^"]+(")',
        rf"\g<1>{version}\2",
        "axiomkit_py Cargo.lock package version",
    )
    PATH_CARGO_LOCK.write_text(updated)


def main() -> None:
    args = parse_args()
    update_cargo_toml(args.version)
    update_cargo_lock(args.version)
    print(f"Synchronized axiomkit_py release version to {args.version}")


if __name__ == "__main__":
    main()
