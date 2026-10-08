#!/bin/bash
# Verifier entry point. Runs fully offline: pytest is baked into the image.
# Installs the grading crash-model module (full durability model), then runs the behavioural
# test-suite against /app/tinykv and writes the reward.
set -u
mkdir -p /logs/verifier
echo 0 > /logs/verifier/reward.txt

cp /tests/fs_verifier.py /app/tinykv/fs.py
find /app -name '__pycache__' -type d -prune -exec rm -rf {} +

if ! python -c "import pytest" 2>/dev/null; then
  echo "pytest is not installed in the image (expected pytest==8.3.4)" >&2
  exit 1
fi

cd /tests || exit 1
python -m pytest -p no:cacheprovider -rA /tests/test_outputs.py
status=$?

if [ "$status" -eq 0 ]; then
  echo 1 > /logs/verifier/reward.txt
fi
exit "$status"
