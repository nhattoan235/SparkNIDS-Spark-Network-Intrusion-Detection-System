import hashlib
import io

from src import download_data


class FakeResponse:
    def __init__(self, content: bytes, declared_length: int, *, status=200, content_range=None):
        self.content = io.BytesIO(content)
        self.headers = {"Content-Length": str(declared_length)}
        if content_range:
            self.headers["Content-Range"] = content_range
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.content.close()

    def read(self, size=-1):
        return self.content.read(size)


def test_download_retries_when_server_closes_before_declared_content_length(tmp_path, monkeypatch):
    content = b"verified dataset bytes"
    calls = []

    def fake_urlopen(request, **_kwargs):
        calls.append(request)
        offset_header = request.headers.get("Range")
        if not offset_header:
            return FakeResponse(content[:7], len(content))
        assert offset_header == "bytes=7-"
        return FakeResponse(content[7:], len(content) - 7, status=206,
            content_range=f"bytes 7-{len(content) - 1}/{len(content)}")

    monkeypatch.setattr(download_data.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(download_data.time, "sleep", lambda _seconds: None)
    destination = tmp_path / "training.csv"

    download_data.download_file(
        "https://data.invalid/training.csv",
        destination,
        hashlib.sha256(content).hexdigest(),
    )

    assert destination.read_bytes() == content
    assert len(calls) == 2
