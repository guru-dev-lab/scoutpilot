#!/bin/bash
# ScoutPilot - Quick Start
# Usage: bash run.sh

cd "$(dirname "$0")"

# Create .env if missing
if [ ! -f .env ]; then
  cp .env.example .env
  echo "Created .env from .env.example"
  echo "Edit .env to add your API keys (optional for basic mode)"
fi

echo ""
echo "  ╔═══════════════════════════════════╗"
echo "  ║     🎯 ScoutPilot Starting...      ║"
echo "  ╚═══════════════════════════════════╝"
echo ""
echo "  Dashboard: http://localhost:8000"
echo "  API docs:  http://localhost:8000/docs"
echo ""

python main.py
