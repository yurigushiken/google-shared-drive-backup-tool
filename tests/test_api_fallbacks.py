import json
import sys
from pathlib import Path

import httplib2
from googleapiclient.errors import HttpError

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


class _FakeAbout:
    def __init__(self, export_formats):
        self.export_formats = export_formats

    def get(self, **kwargs):
        return _Req({"exportFormats": self.export_formats})


class _FakeFiles:
    class _Request:
        def __init__(self, mime_type):
            self.mime_type = mime_type

    def export_media(self, **kwargs):
        return _FakeFiles._Request(kwargs["mimeType"])

    def get_media(self, **kwargs):
        return _FakeFiles._Request("application/octet-stream")


class _FakeDriveService:
    def __init__(self, export_formats):
        self._about = _FakeAbout(export_formats)
        self._files = _FakeFiles()

    def about(self):
        return self._about

    def files(self):
        return self._files


class _FakeFormResponses:
    def list(self, **kwargs):
        return _Req({"responses": [{"responseId": "r1"}]})


class _FakeForms:
    def get(self, **kwargs):
        return _Req({"formId": kwargs["formId"], "info": {"title": "Demo Form"}})

    def responses(self):
        return _FakeFormResponses()


class _FakeFormsService:
    def forms(self):
        return _FakeForms()


class _DeniedForms:
    def get(self, **kwargs):
        raise Exception("PERMISSION_DENIED: Request had insufficient authentication scopes.")

    def responses(self):
        return _FakeFormResponses()


class _DeniedFormsService:
    def forms(self):
        return _DeniedForms()


class _ScopeInsufficientForms:
    def get(self, **kwargs):
        raise Exception(
            "Request had insufficient authentication scopes. "
            "Details: reason ACCESS_TOKEN_SCOPE_INSUFFICIENT"
        )

    def responses(self):
        return _FakeFormResponses()


class _ScopeInsufficientFormsService:
    def forms(self):
        return _ScopeInsufficientForms()


class _DisabledForms:
    def get(self, **kwargs):
        raise Exception("Google Forms API has not been used in project 123 before or it is disabled.")

    def responses(self):
        return _FakeFormResponses()


class _DisabledFormsService:
    def forms(self):
        return _DisabledForms()


def _mk_backup(tmp_path, drive_service, forms_service=None):
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
        drive_service=drive_service,
        forms_service=forms_service,
        shared_drive_id="drive-1",
        target_mirror_path=str(target_mirror),
        metadata_path=str(metadata_path),
        mode="update",
        report_messages=report_messages,
        include_shared_items=False,
    )


def test_download_file_uses_forms_api_for_google_forms(tmp_path):
    drive_service = _FakeDriveService(export_formats={})
    forms_service = _FakeFormsService()
    backup = _mk_backup(tmp_path, drive_service, forms_service=forms_service)

    file_metadata = {}
    base_path = tmp_path / "forms" / "form_a"
    result = backup.download_file(
        file_id="form-1",
        local_path=str(base_path),
        mime_type="application/vnd.google-apps.form",
        file_metadata=file_metadata,
    )

    assert result == "downloaded"
    assert file_metadata["backup_type"] == "forms_api"
    out_path = Path(file_metadata["local_path"])
    assert out_path.exists()
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["form"]["formId"] == "form-1"
    assert len(payload["responses"]) == 1


def test_download_file_falls_back_to_lower_fidelity_export(tmp_path, monkeypatch):
    export_formats = {
        "application/vnd.google-apps.presentation": [
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            "application/pdf",
        ]
    }
    drive_service = _FakeDriveService(export_formats=export_formats)
    backup = _mk_backup(tmp_path, drive_service)

    class _FakeDownloader:
        def __init__(self, fh, request, chunksize):
            self.fh = fh
            self.request = request
            self._done = False

        def next_chunk(self):
            if self.request.mime_type == "application/vnd.openxmlformats-officedocument.presentationml.presentation":
                raise HttpError(httplib2.Response({"status": "403"}), b'{"error":{"message":"exportSizeLimitExceeded"}}')
            if not self._done:
                self.fh.write(b"pdf")
                self._done = True
                return None, True
            return None, True

    monkeypatch.setattr(drive_backup, "MediaIoBaseDownload", _FakeDownloader)

    file_metadata = {}
    base_path = tmp_path / "slides" / "deck"
    result = backup.download_file(
        file_id="slide-1",
        local_path=str(base_path),
        mime_type="application/vnd.google-apps.presentation",
        file_metadata=file_metadata,
    )

    assert result == "downloaded"
    assert file_metadata["export_mime"] == "application/pdf"
    assert file_metadata["local_path"].endswith(".pdf")
    assert Path(file_metadata["local_path"]).exists()


def test_download_file_marks_forms_permission_denied_as_manual_required(tmp_path):
    drive_service = _FakeDriveService(export_formats={})
    forms_service = _DeniedFormsService()
    backup = _mk_backup(tmp_path, drive_service, forms_service=forms_service)

    file_metadata = {}
    base_path = tmp_path / "forms" / "form_denied"
    result = backup.download_file(
        file_id="form-denied",
        local_path=str(base_path),
        mime_type="application/vnd.google-apps.form",
        file_metadata=file_metadata,
    )

    assert result == "skipped_manual_required"
    assert file_metadata["manual_download_required"] is True
    assert file_metadata["last_error_code"] == "forms_api_permission_denied"


def test_download_file_marks_forms_scope_insufficient_as_permission_denied(tmp_path):
    drive_service = _FakeDriveService(export_formats={})
    forms_service = _ScopeInsufficientFormsService()
    backup = _mk_backup(tmp_path, drive_service, forms_service=forms_service)

    file_metadata = {}
    base_path = tmp_path / "forms" / "form_scope_insufficient"
    result = backup.download_file(
        file_id="form-scope-insufficient",
        local_path=str(base_path),
        mime_type="application/vnd.google-apps.form",
        file_metadata=file_metadata,
    )

    assert result == "skipped_manual_required"
    assert file_metadata["manual_download_required"] is True
    assert file_metadata["last_error_code"] == "forms_api_permission_denied"
    assert file_metadata["manual_download_reason"] == "Forms API permission denied (check API enablement/scopes/access)"


def test_download_file_marks_forms_api_not_enabled_as_manual_required(tmp_path):
    drive_service = _FakeDriveService(export_formats={})
    forms_service = _DisabledFormsService()
    backup = _mk_backup(tmp_path, drive_service, forms_service=forms_service)

    file_metadata = {}
    base_path = tmp_path / "forms" / "form_disabled"
    result = backup.download_file(
        file_id="form-disabled",
        local_path=str(base_path),
        mime_type="application/vnd.google-apps.form",
        file_metadata=file_metadata,
    )

    assert result == "skipped_manual_required"
    assert file_metadata["manual_download_required"] is True
    assert file_metadata["last_error_code"] == "forms_api_not_enabled"
