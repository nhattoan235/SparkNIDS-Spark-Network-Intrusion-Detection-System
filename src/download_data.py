"""Download the designated UNSW-NB15 files and verify their published hashes."""

from __future__ import annotations

import argparse
import hashlib
import shutil
import urllib.request
from pathlib import Path
from typing import Any

from src.spark_session import PROJECT_ROOT, load_config


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download_file(url: str, destination: Path, expected_sha256: str, *, force: bool = False) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.is_file() and not force:
        actual = sha256_file(destination)
        if actual == expected_sha256:
            print(f"Verified existing file: {destination.name}")
            return
        raise ValueError(
            f"Existing file has an unexpected SHA-256: {destination} ({actual}). "
            "Use --force only after confirming it may be replaced."
        )

    temporary = destination.with_suffix(destination.suffix + ".download")
    try:
        with urllib.request.urlopen(url) as response, temporary.open("wb") as output:
            shutil.copyfileobj(response, output)
        actual = sha256_file(temporary)
        if actual != expected_sha256:
            raise ValueError(
                f"SHA-256 mismatch for {destination.name}: expected {expected_sha256}, got {actual}"
            )
        temporary.replace(destination)
        print(f"Downloaded and verified: {destination.name}")
    finally:
        temporary.unlink(missing_ok=True)


def download_dataset(config: dict[str, Any], *, force: bool = False) -> None:
    raw_dir = PROJECT_ROOT / config["paths"]["raw"]
    for file_config in config["dataset"]["files"].values():
        download_file(
            file_config["mirror_url"],
            raw_dir / file_config["filename"],
            file_config["sha256"],
            force=force,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download and verify UNSW-NB15 train/test CSVs.")
    parser.add_argument("--force", action="store_true", help="Replace existing files after downloading.")
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    download_dataset(load_config(), force=arguments.force)

