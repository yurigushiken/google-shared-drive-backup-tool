@echo off
cls
echo Running Google Drive Backup Script...
cd "%~dp0"
python "%~dp0drive_backup.py"
if errorlevel 1 (
    echo Backup script encountered errors.
) else (
    echo Backup script completed.
)
pause 