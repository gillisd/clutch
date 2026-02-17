#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "==> Running tests..."
go test ./... -race -count=1 -v

echo "==> Building binaries..."
mkdir -p bin
for cmd in cmd/*/; do
    name=$(basename "$cmd")
    go build -o "bin/$name" "./$cmd"
    echo "    built bin/$name"
done

echo "==> Done."
