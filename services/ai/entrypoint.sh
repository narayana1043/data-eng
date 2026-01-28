#!/bin/sh
set -eu

REQ="/requirements.txt"
STAMP="/venv/.requirements.sha256"

# fix permissions for venv volume
chown -R $(id -u):$(id -g) /venv 2>/dev/null || true

# ensure venv exists (if volume is empty first time)
if [ ! -x "/venv/bin/python" ]; then
  python -m venv /venv
  /venv/bin/pip install --upgrade pip
fi

echo $(whoami)
echo $(which python)

# install only when requirements.txt changes
NEW_SHA="$(sha256sum "$REQ" | awk '{print $1}')"
OLD_SHA="$(cat "$STAMP" 2>/dev/null || true)"

if [ "$NEW_SHA" != "$OLD_SHA" ]; then
  echo "requirements.txt changed (or first run). Installing..."
  /venv/bin/pip install -r "$REQ"
  echo "$NEW_SHA" > "$STAMP"
else
  echo "requirements.txt unchanged. Skipping pip install."
fi

exec "$@"
