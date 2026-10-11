"""Download the designated UNSW-NB15 files and verify their published hashes."""

from __future__ import annotations

import argparse
import hashlib
import re
import shutil
import time
import urllib.request
from pathlib import Path
from typing import Any

from src.spark_session import PROJECT_ROOT, load_config


class IncompleteDownloadError(ValueError):
    pass


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
    for attempt in range(8):
        try:
            offset = temporary.stat().st_size if temporary.exists() else 0
            request = urllib.request.Request(
                url, headers={"Range": f"bytes={offset}-"} if offset else {}
            )
            with urllib.request.urlopen(request, timeout=60) as response:
                status = getattr(response, "status", None)
                if status is None:
                    status = response.getcode()
                if offset and status == 206:
                    content_range = response.headers.get("Content-Range", "")
                    match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", content_range)
                    if not match or int(match.group(1)) != offset:
                        temporary.unlink(missing_ok=True)
                        raise ValueError(f"Invalid Content-Range while resuming {destination.name}")
                    expected_length = int(match.group(3))
                    mode = "ab"
                else:
                    declared_length = response.headers.get("Content-Length")
                    expected_length = int(declared_length) if declared_length else None
                    mode = "wb"
                with temporary.open(mode) as output:
                    shutil.copyfileobj(response, output)
            actual_length = temporary.stat().st_size
            if expected_length is not None and actual_length != expected_length:
                raise IncompleteDownloadError(
                    f"Incomplete download for {destination.name}: received {actual_length} of "
                    f"{expected_length} bytes"
                )
            actual = sha256_file(temporary)
            if actual != expected_sha256:
                temporary.unlink(missing_ok=True)
                raise ValueError(
                    f"SHA-256 mismatch for {destination.name}: expected {expected_sha256}, got {actual}"
                )
            temporary.replace(destination)
            print(f"Downloaded and verified: {destination.name}")
            return
        except IncompleteDownloadError as error:
            if attempt == 7:
                raise
            received = temporary.stat().st_size if temporary.exists() else 0
            print(f"Download interrupted ({error}); retrying from byte {received}.")
            time.sleep(attempt + 1)
        except OSError as error:
            if attempt == 7:
                raise
            received = temporary.stat().st_size if temporary.exists() else 0
            print(f"Download interrupted ({error}); retrying from byte {received}.")
            time.sleep(attempt + 1)


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

