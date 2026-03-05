#!/bin/bash
# run_backup.command

# Get the directory containing this script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Change to the script's directory
cd "$DIR"

echo "Running Google Drive Backup Script..."

# Ensure Python 3 is used (adjust if python3 is not in PATH)
PYTHON_CMD=python3 

# Run the script
"$PYTHON_CMD" "$DIR/drive_backup.py"

# Check the exit code
if [ $? -ne 0 ]; then
  echo ""
  echo "Backup script encountered errors. Check the log file in the 'logs' directory."
else
  echo ""
  echo "Backup script completed successfully."
fi

# Keep terminal open to see output (optional)
# read -p "Press Enter to close..." 