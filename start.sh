#!/bin/bash
set -e

echo "Starting data loader..."
python -m src.loader

echo "Starting bot..."
python -m src.bot
