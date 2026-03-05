# Google Drive Backup Tool

This tool creates local backups of a Google Shared Drive with two modes: `update` and `full`.

## Prerequisites

- Python 3.10+ recommended
- Installed dependencies:
  ```bash
  pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib
  ```
- Local OAuth files (not committed to git):
  - `credentials.json` (OAuth client credentials)
  - `token.pickle` (created automatically after first login)

## Current behavior (as of March 2026)

- `full` mode creates a new timestamped mirror folder and downloads everything.
- `update` mode first tries Google Drive `changes.list` using stored page tokens (`metadata/changes_state.json`) and processes only changed files.
- If no token exists yet (first run) or token is rejected by Google, update mode automatically falls back to full recursive scan for that run, then reseeds token state.
- Deleted files in Drive are not deleted locally (historical preservation).
- Metadata is centralized in `metadata/central_metadata.json`.
- Changes API token state is stored in `metadata/changes_state.json`.
- On Windows, metadata paths are remapped if the drive letter changed (for example `F:` to `D:`) but the same mirror timestamp folder is present.
- For Google Docs/Sheets/Slides exports, the downloader keeps the real exported extension path in metadata (for example `.docx`, `.xlsx`, `.pptx`).

## Directory layout

```text
GoogleDriveBackupTool/
  run_backup.bat
  run_backup.command
  drive_backup.py
  config.json
  credentials.json
  token.pickle
  metadata/
    central_metadata.json
    central_metadata.json.bak
    changes_state.json
  reports/
    backup_report_*.txt
  logs/
    backup_log_*.txt
  mirror/
    mirror-YYYYMMDD-HHMMSS/
```

## Usage

### Windows
1. Double-click `run_backup.bat`.
2. Choose mode:
   - `update`: update latest mirror snapshot (daily usage).
   - `full`: create a new full snapshot.

### macOS
1. Install prerequisites once:
   ```bash
   pip3 install google-api-python-client google-auth-httplib2 google-auth-oauthlib
   ```
2. Make launcher executable once:
   ```bash
   chmod +x run_backup.command
   ```
3. Run `run_backup.command` and choose `update` or `full`.

### CLI overrides (optional)

```bash
python drive_backup.py --config config.json --report-dir reports --log-dir logs
```

Supported overrides:
- `--config`
- `--drive-id`
- `--output-dir`
- `--report-dir`
- `--log-dir`

## Configuration (`config.json`)

```json
{
  "shared_drive_id": "YOUR_SHARED_DRIVE_ID",
  "mirror_root_path": "mirror",
  "report_dir": "reports",
  "log_dir": "logs",
  "include_shared_items": false,
  "calculate_drive_totals_before_backup": false,
  "use_changes_api_on_update": true,
  "changes_page_size": 1000
}
```

If `config.json` is missing, internal fallback defaults are used. In that fallback path, `mirror_root_path` defaults to `backups` (not `mirror`).

### Config notes

- `calculate_drive_totals_before_backup`:
  - `false` (recommended): skips expensive full-drive size/count pre-scan to improve update start time.
  - `true`: runs pre-scan and adds drive totals/percentage info to reports.
- `use_changes_api_on_update`:
  - `true` (recommended): update mode uses Drive Changes API token-based incremental sync.
  - `false`: update mode uses the older full recursive listing approach.
- `changes_page_size`:
  - Changes API page size (default `1000`).

## Performance notes

- After token seeding, most update runs avoid full recursive listing and are substantially faster on large drives.
- If token state is missing/invalid, one fallback run may still take longer (full recursive scan).
- The tool uses retry + exponential backoff for transient API/network issues.
- API export limits still apply to some Google Workspace files; these appear in report errors.

## Testing

Run the local test suite:

```bash
pytest -q tests
```

Compile check:

```bash
python -m py_compile drive_backup.py
```

## Git/backup hygiene

- `.gitignore` excludes secrets and generated runtime data (`metadata/`, `mirror/`, `reports/`, `logs/`, backups, cache dirs).
- Keep `credentials.json`, `token.pickle`, and large backup outputs out of source control.

## Rollback / safety

If a new code change does not work:

1. Restore from local code backup copies (for example `manual_backup_YYYYMMDD-HHMMSS/drive_backup.py.bak`).
2. Restore metadata from `metadata/central_metadata.json.bak` if needed.
3. Existing `mirror/mirror-*` snapshots are preserved; they are not overwritten by rollback.

## Initial authentication

On first run, browser OAuth will open. Sign in and authorize Drive read access.
