# Google Shared Drive Backup Tool

Backs up a Google Shared Drive to a local mirror with two modes:
- `update`: fast incremental sync (Drive Changes API + targeted retries)
- `full`: new full snapshot baseline

## Status (March 21, 2026)

- `update` and `full` behaviors are covered by tests.
- Google Forms are backed up via Forms API as `*.form.json`.
- Lower-fidelity export fallback is enabled for large Workspace files:
  - Slides: `pptx -> pdf/text`
  - Sheets: `xlsx -> pdf/csv`
  - Docs: `docx -> pdf/txt`
- Unresolved retry includes stale-metadata local-path gaps.

## Prerequisites

- Python 3.10+ recommended
- Install dependencies:
  ```bash
  pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib
  ```
- OAuth client credentials file (one of):
  - `credentials.json` in repo root, or
  - `client_secret.json` in repo root, or
  - profile-local `tokens/<profile>/credentials.json`

## Multi-User Token Profiles

Each lab member should use a separate token profile:

```text
tokens/
  yuri/
    token.pickle
  alice/
    token.pickle
  bob/
    token.pickle
```

Current project default is:
- `token_profile: "yuri"`
- `tokens_root_dir: "tokens"`

You can switch active user in either way:
- Edit `config.json` field `token_profile`
- Or override at runtime: `--token-profile <name>`

## Directory Layout

```text
GoogleDriveBackupTool/
  drive_backup.py
  config.json
  credentials.json (optional shared OAuth client file)
  metadata/
    central_metadata.json
    changes_state.json
  mirror/
    mirror-YYYYMMDD-HHMMSS/
  reports/
    backup_report_*.txt
    unresolved_files_*.csv
  logs/
    backup_log_*.txt
  tokens/
    <profile>/
      token.pickle
```

## Usage

### Windows
1. Run `run_backup.bat`
2. Choose `update` or `full`

### macOS/Linux
```bash
python drive_backup.py
```

### Useful CLI Overrides

```bash
python drive_backup.py --config config.json --token-profile yuri --log-level INFO
```

Supported overrides:
- `--config`
- `--drive-id`
- `--output-dir`
- `--report-dir`
- `--log-dir`
- `--log-level`
- `--token-profile`
- `--tokens-root-dir`
- `--credentials-file`
- `--token-file`
- `--max-unresolved-retries`
- `--retry-manual-required`
- `--disable-forms-api-backup`

## Configuration (`config.json`)

```json
{
  "shared_drive_id": "YOUR_SHARED_DRIVE_ID",
  "mirror_root_path": "mirror",
  "report_dir": "reports",
  "log_dir": "logs",
  "tokens_root_dir": "tokens",
  "token_profile": "yuri",
  "log_level": "INFO",
  "include_shared_items": false,
  "calculate_drive_totals_before_backup": false,
  "use_changes_api_on_update": true,
  "changes_page_size": 1000,
  "metadata_save_every_items": 500,
  "metadata_save_min_seconds": 300,
  "retry_unresolved_missing_files": true,
  "max_unresolved_retries_per_run": 200,
  "retry_manual_required_files": false,
  "generate_unresolved_report": true,
  "enable_forms_api_backup": true
}
```

## Team Onboarding (OAuth + API Setup)

Use the step-by-step web guide in [`docs/index.html`](docs/index.html).

Expected GitHub Pages URL for this repo:
- `https://yurigushiken.github.io/google-shared-drive-backup-tool/`

## Recommended Workflow

1. Use `update` routinely.
2. Use `full` only for intentional baseline snapshots.
3. Review `reports/backup_report_*.txt`.
4. If needed, review unresolved CSV:
   - `reports/unresolved_files_<timestamp>.csv`

## Tests

```bash
python -m pytest -q
python -m py_compile drive_backup.py
```

## Security / Git Hygiene

- Never commit `token.pickle` or personal credentials.
- Keep `metadata/`, `mirror/`, `reports/`, `logs/` out of source control.

