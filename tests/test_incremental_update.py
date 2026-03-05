import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from drive_backup import (  # noqa: E402
    get_effective_local_path,
    resolve_existing_local_path,
    strip_unc_prefix,
    should_calculate_drive_totals,
)


def test_resolve_existing_local_path_remaps_drive_letter_to_current_mirror(tmp_path):
    target_mirror = tmp_path / "mirror" / "mirror-20250709-154056"
    expected_file = target_mirror / "folder" / "file.txt"
    expected_file.parent.mkdir(parents=True, exist_ok=True)
    expected_file.write_text("ok", encoding="utf-8")

    old_path = r"\\?\F:\GoogleDriveBackupTool\mirror\mirror-20250709-154056\folder\file.txt"

    resolved = resolve_existing_local_path(old_path, str(target_mirror))

    assert os.path.normcase(strip_unc_prefix(resolved)) == os.path.normcase(str(expected_file))


def test_resolve_existing_local_path_keeps_existing_original_path(tmp_path):
    old_file = tmp_path / "old" / "keep.txt"
    old_file.parent.mkdir(parents=True, exist_ok=True)
    old_file.write_text("ok", encoding="utf-8")

    target_mirror = tmp_path / "mirror" / "mirror-20250709-154056"
    target_mirror.mkdir(parents=True, exist_ok=True)

    resolved = resolve_existing_local_path(str(old_file), str(target_mirror))

    assert os.path.normcase(strip_unc_prefix(resolved)) == os.path.normcase(str(old_file))


def test_get_effective_local_path_prefers_downloaded_metadata_path():
    file_metadata = {"local_path": r"D:\mirror\doc.docx"}
    requested_local_path = r"D:\mirror\doc"

    assert get_effective_local_path(file_metadata, requested_local_path) == r"D:\mirror\doc.docx"


def test_should_calculate_drive_totals_defaults_to_false_and_honors_true():
    assert should_calculate_drive_totals({}) is False
    assert should_calculate_drive_totals({"calculate_drive_totals_before_backup": True}) is True
