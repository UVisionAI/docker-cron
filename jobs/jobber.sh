#!/usr/bin/env bash
set -e

if [ -z "$1" ]; then
  echo "Usage: $0 <script_name.py> [args...]"
  exit 1
fi

SCRIPT_NAME=$1
shift # Remove the first argument, leaving the rest

cd /opt/jobs
source virtualenv/bin/activate

if [ ! -f "$SCRIPT_NAME" ]; then
  echo "Error: Script '$SCRIPT_NAME' not found in /opt/jobs/"
  exit 1
fi

python3 "$SCRIPT_NAME" "$@" >> /opt/jobs/logs/"${SCRIPT_NAME%.py}.log" 2>&1

