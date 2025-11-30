#!/usr/bin/env bash

set -e

echo "Installing required database drivers (clickhouse-connect)..."
pip install clickhouse-connect

echo "Starting original Superset initialization (/app/docker/docker-init.sh)..."
/app/docker/docker-init.sh

echo "Superset initialization with dependencies finished."