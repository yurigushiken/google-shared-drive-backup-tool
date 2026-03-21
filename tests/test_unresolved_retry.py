import json
import os
import sys
from pathlib import Path

import httplib2
from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from drive_backup import DriveBackup, generate_unresolved_files_report  # noqa: E402


class _Req:
    def __init__(self, payload):
        self.payload = payload

    def execute(self):
        return self.payload


class _FakeAbout:
    def get(self, **kwargs):
        return _Req(
            {
                "exportFormats": {
                    "application/vnd.google-apps.spreadsheet": [
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    ]
                }
            }
        )


class _FakeFiles:
    def __init__(self, file_payloads):
        self.file_payloads = file_payloads

    def get(self, **kwargs):
        payload = self.file_payloads[kwargs["fileId"]]
        if isinstance(payload, Exception):
            raise payload
        return _Req(payload)


class _FakeDriveService:
    def __init__(self, file_payloads):
        self._files = _FakeFiles(file_payloads)
        self._about = _FakeAbout()

    def files(self):
        return self._files

    def about(self):
        return self._about


def _mk_backup(tmp_path, metadata, service, retry_manual_required_files=False):
    target_mirror = tmp_path / "mirror" / "mirror-20260131-120000"
    target_mirror.mkdir(parents=True, exist_ok=True)
    metadata_path = tmp_path / "metadata.json"
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

    report_messages = {
        "summary": [],
        "details": [],
        "errors": [],
        "manual_download_files": [],
        "all_error_logs": [],
    }

    return DriveBackup(
        drive_service=service,
        shared_drive_id="drive-1",
        target_mirror_path=str(target_mirror),
        metadata_path=str(metadata_path),
        mode="update",
        report_messages=report_messages,
        include_shared_items=False,
        sync_state_path=str(tmp_path / "changes_state.json"),
        use_changes_api_on_update=True,
        changes_page_size=1000,
        retry_unresolved_missing_files=True,
        max_unresolved_retries_per_run=200,
        retry_manual_required_files=retry_manual_required_files,
    )


def test_generate_unresolved_files_report_counts_missing_and_manual(tmp_path):
    existing_file = tmp_path / "existing.txt"
    existing_file.write_text("x", encoding="utf-8")

    metadata = {
        "a": {
            "name": "A",
            "error": "boom",
            "local_path": "",
            "manual_download_required": True,
            "manual_download_reason": "Drive API export size limit (10 MB)",
        },
        "b": {
            "name": "B",
            "error": "bad",
            "local_path": str(existing_file),
        },
        "c": {
            "name": "C",
            "error": "HttpError 403 exportSizeLimitExceeded",
            "local_path": "",
        },
        "d": {
            "name": "D",
            "mime_type": "application/pdf",
            "local_path": str(tmp_path / "missing.pdf"),
        },
        "e": {
            "name": "E removed",
            "mime_type": "application/pdf",
            "local_path": str(tmp_path / "missing_removed.pdf"),
            "removed": True,
            "error": "stale",
        },
    }

    result = generate_unresolved_files_report(metadata, str(tmp_path), "2026-03-21_15-00-00")

    assert result["total"] == 4
    assert result["missing_local"] == 3
    assert result["manual_required"] == 2
    assert result["with_local_copy"] == 1
    assert Path(result["report_path"]).exists()


def test_retry_unresolved_files_downloads_missing_error_entries(tmp_path):
    metadata = {
        "file-1": {
            "name": "Sheet A",
            "mime_type": "application/vnd.google-apps.spreadsheet",
            "modified_time": "2026-03-01T10:00:00.000Z",
            "error": "previous failure",
        }
    }
    service = _FakeDriveService(
        {
            "file-1": {
                "id": "file-1",
                "name": "Sheet A",
                "mimeType": "application/vnd.google-apps.spreadsheet",
                "modifiedTime": "2026-03-01T10:00:00.000Z",
                "md5Checksum": "abc",
                "size": "12",
                "parents": ["folder-1"],
                "trashed": False,
            },
            "folder-1": {
                "id": "folder-1",
                "name": "Folder A",
                "mimeType": "application/vnd.google-apps.folder",
                "parents": ["drive-1"],
                "trashed": False,
            },
        }
    )
    backup = _mk_backup(tmp_path, metadata, service)

    def _fake_download(_file_id, local_path, _mime_type, file_metadata):
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path + ".xlsx", "wb") as f:
            f.write(b"x")
        file_metadata["local_path"] = local_path + ".xlsx"
        return "downloaded"

    backup.download_file = _fake_download
    stats = backup.retry_unresolved_files(str(tmp_path / "mirror" / "mirror-20260131-120000" / "Team Drive"))

    assert stats["attempted"] == 1
    assert stats["downloaded"] == 1
    assert stats["errors"] == 0


def test_retry_unresolved_files_downloads_stale_missing_local_path_without_error(tmp_path):
    metadata = {
        "file-1": {
            "name": "PDF A",
            "mime_type": "application/pdf",
            "modified_time": "2026-03-01T10:00:00.000Z",
            "local_path": str(tmp_path / "missing-folder" / "pdf_a.pdf"),
        }
    }
    service = _FakeDriveService(
        {
            "file-1": {
                "id": "file-1",
                "name": "PDF A",
                "mimeType": "application/pdf",
                "modifiedTime": "2026-03-01T10:00:00.000Z",
                "md5Checksum": "abc",
                "size": "12",
                "parents": ["folder-1"],
                "trashed": False,
            },
            "folder-1": {
                "id": "folder-1",
                "name": "Folder A",
                "mimeType": "application/vnd.google-apps.folder",
                "parents": ["drive-1"],
                "trashed": False,
            },
        }
    )
    backup = _mk_backup(tmp_path, metadata, service)

    def _fake_download(_file_id, local_path, _mime_type, file_metadata):
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(b"x")
        file_metadata["local_path"] = local_path
        return "downloaded"

    backup.download_file = _fake_download
    stats = backup.retry_unresolved_files(str(tmp_path / "mirror" / "mirror-20260131-120000" / "Team Drive"))

    assert stats["attempted"] == 1
    assert stats["downloaded"] == 1
    assert stats["errors"] == 0


def test_retry_unresolved_skips_manual_required_by_default(tmp_path):
    metadata = {
        "file-1": {
            "name": "Form A",
            "mime_type": "application/vnd.google-apps.form",
            "modified_time": "2026-03-01T10:00:00.000Z",
            "error": "conversion unsupported",
            "manual_download_required": True,
        }
    }
    service = _FakeDriveService({})
    backup = _mk_backup(tmp_path, metadata, service, retry_manual_required_files=False)

    stats = backup.retry_unresolved_files(str(tmp_path / "mirror" / "mirror-20260131-120000" / "Team Drive"))

    assert stats["attempted"] == 0


def test_retry_unresolved_skips_inferred_manual_required_errors(tmp_path):
    metadata = {
        "file-1": {
            "name": "Big Sheet",
            "mime_type": "application/vnd.google-apps.spreadsheet",
            "modified_time": "2026-03-01T10:00:00.000Z",
            "error": "HttpError 403 exportSizeLimitExceeded",
        }
    }
    service = _FakeDriveService({})
    backup = _mk_backup(tmp_path, metadata, service, retry_manual_required_files=False)

    stats = backup.retry_unresolved_files(str(tmp_path / "mirror" / "mirror-20260131-120000" / "Team Drive"))

    assert stats["attempted"] == 0


def test_retry_unresolved_marks_not_found_entries_as_removed(tmp_path):
    metadata = {
        "file-1": {
            "name": "Missing legacy file",
            "mime_type": "application/pdf",
            "modified_time": "2026-03-01T10:00:00.000Z",
            "error": "legacy missing path",
            "local_path": str(tmp_path / "missing.pdf"),
        }
    }
    not_found = HttpError(
        httplib2.Response({"status": "404"}),
        b'{"error":{"message":"File not found","errors":[{"reason":"notFound"}]}}',
    )
    service = _FakeDriveService({"file-1": not_found})
    backup = _mk_backup(tmp_path, metadata, service)

    stats = backup.retry_unresolved_files(str(tmp_path / "mirror" / "mirror-20260131-120000" / "Team Drive"))

    assert stats["attempted"] == 1
    assert stats["downloaded"] == 0
    assert stats["skipped"] == 1
    assert stats["errors"] == 0
    assert backup.metadata["file-1"]["removed"] is True
    assert "error" not in backup.metadata["file-1"]


def test_retry_unresolved_skips_entries_already_marked_removed(tmp_path):
    metadata = {
        "file-1": {
            "name": "Already removed",
            "mime_type": "application/pdf",
            "modified_time": "2026-03-01T10:00:00.000Z",
            "local_path": str(tmp_path / "missing.pdf"),
            "removed": True,
            "error": "legacy",
        }
    }
    service = _FakeDriveService({})
    backup = _mk_backup(tmp_path, metadata, service)

    stats = backup.retry_unresolved_files(str(tmp_path / "mirror" / "mirror-20260131-120000" / "Team Drive"))

    assert stats["attempted"] == 0
