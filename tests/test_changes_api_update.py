import json
import os
import sys
from pathlib import Path

import httplib2
from googleapiclient.errors import HttpError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from drive_backup import DriveBackup, load_sync_state  # noqa: E402


class _Req:
    def __init__(self, payload=None, error=None):
        self.payload = payload
        self.error = error

    def execute(self):
        if self.error:
            raise self.error
        return self.payload


class _FakeChanges:
    def __init__(self, list_payloads=None, list_error=None, start_token="new-token"):
        self.list_payloads = list_payloads or []
        self.list_error = list_error
        self.start_token = start_token
        self.list_calls = []
        self.get_start_calls = []
        self._i = 0

    def list(self, **kwargs):
        self.list_calls.append(kwargs)
        if self.list_error is not None:
            return _Req(error=self.list_error)
        payload = self.list_payloads[min(self._i, len(self.list_payloads) - 1)]
        self._i += 1
        return _Req(payload=payload)

    def getStartPageToken(self, **kwargs):
        self.get_start_calls.append(kwargs)
        return _Req(payload={"startPageToken": self.start_token})


class _FakeFiles:
    def __init__(self, folders):
        self.folders = folders
        self.get_calls = []

    def get(self, **kwargs):
        self.get_calls.append(kwargs)
        file_id = kwargs["fileId"]
        return _Req(payload=self.folders[file_id])


class _FakeDriveService:
    def __init__(self, changes_resource, files_resource):
        self._changes = changes_resource
        self._files = files_resource

    def changes(self):
        return self._changes

    def files(self):
        return self._files


def _mk_backup(tmp_path, service, sync_state):
    target_mirror = tmp_path / "mirror" / "mirror-20260131-120000"
    target_mirror.mkdir(parents=True, exist_ok=True)
    metadata_path = tmp_path / "metadata.json"
    sync_state_path = tmp_path / "changes_state.json"
    sync_state_path.write_text(json.dumps(sync_state), encoding="utf-8")

    report_messages = {"summary": [], "details": [], "errors": [], "manual_download_files": [], "all_error_logs": []}
    backup = DriveBackup(
        drive_service=service,
        shared_drive_id="drive-1",
        target_mirror_path=str(target_mirror),
        metadata_path=str(metadata_path),
        mode="update",
        report_messages=report_messages,
        include_shared_items=False,
        sync_state_path=str(sync_state_path),
        use_changes_api_on_update=True,
        changes_page_size=1000,
    )
    return backup, target_mirror, sync_state_path


def test_changes_update_returns_none_when_no_saved_token(tmp_path):
    changes = _FakeChanges()
    files = _FakeFiles({})
    service = _FakeDriveService(changes, files)
    backup, target_mirror, _ = _mk_backup(tmp_path, service, sync_state={})

    result = backup.run_update_from_changes(str(target_mirror))

    assert result is None
    assert len(changes.list_calls) == 0


def test_changes_update_downloads_changed_file_and_saves_new_token(tmp_path):
    list_payload = {
        "changes": [
            {
                "fileId": "file-1",
                "removed": False,
                "file": {
                    "id": "file-1",
                    "name": "doc1",
                    "mimeType": "application/pdf",
                    "modifiedTime": "2026-03-01T10:00:00.000Z",
                    "md5Checksum": "abc",
                    "size": "12",
                    "parents": ["folder-1"],
                    "trashed": False,
                },
            }
        ],
        "newStartPageToken": "token-new",
    }
    changes = _FakeChanges(list_payloads=[list_payload], start_token="token-new")
    files = _FakeFiles(
        {
            "folder-1": {
                "id": "folder-1",
                "name": "Folder A",
                "mimeType": "application/vnd.google-apps.folder",
                "parents": ["drive-1"],
                "trashed": False,
            }
        }
    )
    service = _FakeDriveService(changes, files)
    backup, target_mirror, sync_state_path = _mk_backup(tmp_path, service, sync_state={"last_start_page_token": "token-old"})

    def _fake_download(file_id, local_path, mime_type, file_metadata):
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(b"x")
        file_metadata["local_path"] = local_path
        return "downloaded"

    backup.download_file = _fake_download

    result = backup.run_update_from_changes(str(target_mirror))

    assert result is not None
    assert result["downloaded"] == 1
    assert len(changes.list_calls) == 1
    params = changes.list_calls[0]
    assert params["supportsAllDrives"] is True
    assert params["driveId"] == "drive-1"
    saved_state = load_sync_state(str(sync_state_path))
    assert saved_state["last_start_page_token"] == "token-new"
    assert "file-1" in backup.metadata
    assert "Folder A" in backup.metadata["file-1"]["local_path"]


def test_changes_update_invalid_token_falls_back_to_full_scan(tmp_path):
    error = HttpError(httplib2.Response({"status": "410"}), b'{"error":{"message":"invalid page token"}}')
    changes = _FakeChanges(list_error=error)
    files = _FakeFiles({})
    service = _FakeDriveService(changes, files)
    backup, target_mirror, _ = _mk_backup(tmp_path, service, sync_state={"last_start_page_token": "token-old"})

    result = backup.run_update_from_changes(str(target_mirror))

    assert result is None
    assert len(changes.list_calls) == 1
