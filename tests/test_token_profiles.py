import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import drive_backup  # noqa: E402


def test_resolve_auth_paths_uses_profile_token_and_root_credentials(tmp_path, monkeypatch):
    monkeypatch.setattr(drive_backup, "script_dir", str(tmp_path))
    (tmp_path / "credentials.json").write_text("{}", encoding="utf-8")

    resolved = drive_backup.resolve_auth_paths(
        config={"token_profile": "yuri", "tokens_root_dir": "tokens"}
    )

    assert resolved["profile"] == "yuri"
    assert resolved["token_file"].endswith("tokens\\yuri\\token.pickle")
    assert resolved["credentials_file"].endswith("credentials.json")
    assert Path(resolved["profile_dir"]).exists()


def test_resolve_auth_paths_prefers_profile_credentials(tmp_path, monkeypatch):
    monkeypatch.setattr(drive_backup, "script_dir", str(tmp_path))
    profile_dir = tmp_path / "tokens" / "alice"
    profile_dir.mkdir(parents=True, exist_ok=True)
    (profile_dir / "credentials.json").write_text('{"installed":{}}', encoding="utf-8")
    (tmp_path / "credentials.json").write_text("{}", encoding="utf-8")

    resolved = drive_backup.resolve_auth_paths(
        config={"token_profile": "alice", "tokens_root_dir": "tokens"}
    )

    assert resolved["credentials_file"].endswith("tokens\\alice\\credentials.json")


def test_resolve_auth_paths_honors_explicit_paths(tmp_path, monkeypatch):
    monkeypatch.setattr(drive_backup, "script_dir", str(tmp_path))
    explicit_credentials = tmp_path / "secrets" / "my_credentials.json"
    explicit_token = tmp_path / "secrets" / "my_token.pickle"
    explicit_credentials.parent.mkdir(parents=True, exist_ok=True)
    explicit_credentials.write_text('{"installed":{}}', encoding="utf-8")

    resolved = drive_backup.resolve_auth_paths(
        config={"token_profile": "alice", "tokens_root_dir": "tokens"},
        credentials_file=str(explicit_credentials),
        token_file=str(explicit_token),
    )

    assert resolved["credentials_file"] == str(explicit_credentials)
    assert resolved["token_file"] == str(explicit_token)


def test_resolve_auth_paths_rejects_invalid_profile_name(tmp_path, monkeypatch):
    monkeypatch.setattr(drive_backup, "script_dir", str(tmp_path))
    (tmp_path / "credentials.json").write_text("{}", encoding="utf-8")

    with pytest.raises(ValueError):
        drive_backup.resolve_auth_paths(
            config={"token_profile": "../bad", "tokens_root_dir": "tokens"}
        )
