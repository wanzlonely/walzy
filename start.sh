#!/bin/bash
cd "$(dirname "$0")"
pip install -r requirements.txt -q --break-system-packages 2>/dev/null || pip install -r requirements.txt -q
python main.py
