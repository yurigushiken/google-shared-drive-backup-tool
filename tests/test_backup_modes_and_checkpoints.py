import json
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import drive_backup  # noqa: E402
from drive_backup import DriveBackup  # noqa: E402


class _Req:
    def __init__(self, payload):
        self.payload = payload

    def execute(self):
        return self.payload


class _FakeDrives:
    def get(self, **kwargs):
        return _Req({"name": "Team Drive"})


class _FakeDriveService:
    def drives(self):
        return _FakeDrives()


class _ListFiles:
    def __init__(self, payload):
        self.payload = payload

    def list(self, **kwargs):
        return _Req(self.payload)


class _DriveServiceWithFiles(_FakeDriveService):
    def __init__(self, payload):
        self._files = _ListFiles(payload)

    def files(self):
        return self._files


def _mk_backup(
    tmp_path,
    mode="update",
    use_changes_api_on_update=True,
    metadata_save_every_items=10,
    metadata_save_min_seconds=300,
    service=None,
):
    target_mirror = tmp_path / "mirror" / "mirror-20260131-120000"
    target_mirror.mkdir(parents=True, exist_ok=True)
    metadata_path = tmp_path / "metadata.json"
    metadata_path.write_text("{}", encoding="utf-8")

    report_messages = {
        "summary": [],
        "details": [],
        "errors": [],
        "manual_download_files": [],
        "all_error_logs": [],
    }

    return DriveBackup(
        drive_service=service or _FakeDriveService(),
        shared_drive_id="drive-1",
        target_mirror_path=str(target_mirror),
        metadata_path=str(metadata_path),
        mode=mode,
        report_messages=report_messages,
        include_shared_items=False,
        sync_state_path=str(tmp_path / "changes_state.json"),
        use_changes_api_on_update=use_changes_api_on_update,
        changes_page_size=1000,
        metadata_save_every_items=metadata_save_every_items,
        metadata_save_min_seconds=metadata_save_min_seconds,
    )


def test_run_backup_update_prefers_changes_api_and_skips_recursive_fallback(monkeypatch, tmp_path):
    backup = _mk_backup(tmp_path, mode="update", use_changes_api_on_update=True)

    def _fake_changes(_mirror_path):
        backup.total_downloaded += 3
        return {"downloaded": 3, "skipped": 0, "errors": 0, "total_size": 0}

    monkeypatch.setattr(backup, "run_update_from_changes", _fake_changes)
    monkeypatch.setattr(
        backup,
        "process_folder",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("process_folder should not run")),
    )
    monkeypatch.setattr(backup, "_save_latest_start_page_token", lambda: None)

    save_calls = []

    def _fake_save_metadata(*, force=False, create_backup=False):
        save_calls.append((force, create_backup))
        return True

    monkeypatch.setattr(backup, "save_metadata", _fake_save_metadata)

    result = backup.run_backup()

    assert result["downloaded"] == 3
    assert any("Update Strategy: Drive changes API" in line for line in backup.report_messages["summary"])
    assert save_calls == [(True, True)]


def test_run_backup_update_falls_back_to_recursive_scan_when_changes_unavailable(monkeypatch, tmp_path):
    backup = _mk_backup(tmp_path, mode="update", use_changes_api_on_update=True)

    monkeypatch.setattr(backup, "run_update_from_changes", lambda _mirror_path: None)

    def _fake_process_folder(**kwargs):
        backup.total_downloaded += 2
        backup.total_skipped += 5
        backup.total_errors += 1
        backup.total_size += 42
        return {"downloaded": 2, "skipped": 5, "errors": 1, "total_size": 42}

    monkeypatch.setattr(backup, "process_folder", _fake_process_folder)
    monkeypatch.setattr(backup, "_save_latest_start_page_token", lambda: None)
    monkeypatch.setattr(backup, "save_metadata", lambda **kwargs: True)

    result = backup.run_backup()

    assert result["downloaded"] == 2
    assert result["skipped"] == 5
    assert result["errors"] == 1
    assert any(
        "Update Strategy: Full recursive fallback (changes API unavailable)" in line
        for line in backup.report_messages["summary"]
    )


def test_run_backup_full_mode_does_not_call_changes_api(monkeypatch, tmp_path):
    backup = _mk_backup(tmp_path, mode="full", use_changes_api_on_update=True)
    called = {"changes": False}

    def _fake_changes(_mirror_path):
        called["changes"] = True
        return None

    def _fake_process_folder(**kwargs):
        backup.total_downloaded += 7
        return {"downloaded": 7, "skipped": 0, "errors": 0, "total_size": 0}

    monkeypatch.setattr(backup, "run_update_from_changes", _fake_changes)
    monkeypatch.setattr(backup, "process_folder", _fake_process_folder)
    monkeypatch.setattr(backup, "_save_latest_start_page_token", lambda: None)
    monkeypatch.setattr(backup, "save_metadata", lambda **kwargs: True)

    result = backup.run_backup()

    assert called["changes"] is False
    assert result["downloaded"] == 7


def test_save_metadata_skips_disk_write_when_not_dirty(monkeypatch, tmp_path):
    backup = _mk_backup(tmp_path)
    backup.metadata_dirty = False
    write_calls = []

    def _fake_save_metadata(_metadata, _path, create_backup=True):
        write_calls.append(create_backup)

    monkeypatch.setattr(drive_backup, "save_metadata", _fake_save_metadata)

    assert backup.save_metadata() is True
    assert write_calls == []

    backup.metadata_dirty = True
    assert backup.save_metadata(create_backup=False) is True
    assert write_calls == [False]


def test_checkpoint_uses_configured_item_interval(monkeypatch, tmp_path):
    backup = _mk_backup(tmp_path, metadata_save_every_items=3)
    backup.metadata_dirty = True
    calls = []

    def _fake_save(*, force=False, create_backup=False):
        calls.append((force, create_backup))
        return True

    monkeypatch.setattr(backup, "save_metadata", _fake_save)

    assert backup.maybe_checkpoint_metadata(items_processed=1) is False
    assert backup.maybe_checkpoint_metadata(items_processed=3) is True
    assert calls == [(False, False)]


def test_create_new_mirror_directory_uses_supplied_root(tmp_path):
    custom_root = tmp_path / "custom-mirror-root"
    created = drive_backup.create_new_mirror_directory(str(custom_root))

    assert Path(drive_backup.strip_unc_prefix(created)).parent == custom_root
    assert Path(drive_backup.strip_unc_prefix(created)).exists()


def test_get_latest_mirror_directory_uses_supplied_root(tmp_path):
    custom_root = tmp_path / "custom-mirror-root"
    custom_root.mkdir(parents=True, exist_ok=True)
    first = custom_root / "mirror-20260101-010101"
    second = custom_root / "mirror-20260101-010102"
    first.mkdir()
    second.mkdir()

    latest = drive_backup.get_latest_mirror_directory(str(custom_root))

    assert latest is not None
    assert Path(drive_backup.strip_unc_prefix(latest)).name == "mirror-20260101-010102"


def test_setup_logging_uses_configured_level(tmp_path):
    log_file = tmp_path / "backup.log"
    drive_backup.setup_logging(str(log_file), log_level="INFO")

    root_logger = logging.getLogger()
    assert root_logger.level == logging.INFO

    for handler in list(root_logger.handlers):
        handler.close()
        root_logger.removeHandler(handler)


def test_process_folder_counts_error_when_download_returns_error(tmp_path):
    list_payload = {
        "files": [
            {
                "id": "file-1",
                "name": "sample.txt",
                "mimeType": "text/plain",
                "modifiedTime": "2026-03-01T10:00:00.000Z",
                "md5Checksum": "abc",
                "size": "10",
            }
        ],
        "nextPageToken": None,
    }
    service = _DriveServiceWithFiles(list_payload)
    backup = _mk_backup(tmp_path, mode="full", service=service)

    def _fake_download(_file_id, _local_path, _mime_type, file_metadata):
        file_metadata["error"] = "boom"
        return "error"

    backup.download_file = _fake_download
    result = backup.process_folder("drive-1", str(tmp_path / "target"))

    assert result["errors"] == 1
    assert any("Error downloading sample.txt: boom" in msg for msg in backup.report_messages["errors"])
