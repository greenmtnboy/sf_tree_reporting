#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Check for API key
if [ -z "${GEMINI_API_KEY:-}" ]; then
  echo ""
  echo "  ERROR: GEMINI_API_KEY is not set."
  echo ""
  echo "  Get a free key at: https://aistudio.google.com/apikey"
  echo "  Then run:  export GEMINI_API_KEY='your-key-here'"
  echo ""
  exit 1
fi

# Create venv if needed
if [ ! -d ".venv" ]; then
  echo "Creating virtual environment..."
  python3 -m venv .venv
fi

source .venv/bin/activate

echo "Installing dependencies..."
pip install -q -r requirements.txt

echo ""
echo "Starting Office Layout Generator..."
echo "Open http://localhost:5000 in your browser"
echo ""

python app.py
