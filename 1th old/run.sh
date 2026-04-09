#!/bin/bash
PYTHON="/Users/shawnclaw/autobot/aiSpeechMulti/venv/bin/python"
SCRIPT="$(dirname "$0")/congress_tracker.py"
"$PYTHON" "$SCRIPT" "$@"
