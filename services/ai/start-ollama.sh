#!/bin/sh
set -e

: "${OLLAMA_MODEL:=phi3.mini}"

ollama serve &
sleep 2

if ! ollama list | awk '{print $1}' | grep -qx "$OLLAMA_MODEL"; then
  echo "Pulling $OLLAMA_MODEL..."
  ollama pull "$OLLAMA_MODEL"
else
  echo "$OLLAMA_MODEL already present."
fi

wait
