#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

PASS=0
FAIL=0
BINDIR="$(pwd)/bin"

cleanup() {
    # Kill background processes if still running.
    [[ -n "${ECHO_PID:-}" ]] && kill "$ECHO_PID" 2>/dev/null || true
    [[ -n "${API_PID:-}" ]]  && kill "$API_PID"  2>/dev/null || true
    wait 2>/dev/null || true
    rm -rf "$BINDIR"
}
trap cleanup EXIT

# Build binaries.
mkdir -p "$BINDIR"
go build -o "$BINDIR/wsecho"    ./cmd/wsecho
go build -o "$BINDIR/apiserver" ./cmd/apiserver

# Start wsecho server.
exec 3< <("$BINDIR/wsecho" -addr :19090 2>&1)
ECHO_PID=$!
sleep 0.5

# Start apiserver.
exec 4< <("$BINDIR/apiserver" -addr :18080 -ws ws://localhost:19090/ws 2>&1)
API_PID=$!
sleep 0.5

check() {
    local name=$1 expected=$2 actual=$3
    if echo "$actual" | grep -q "$expected"; then
        echo "  PASS: $name"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $name"
        echo "    expected to contain: $expected"
        echo "    got: $actual"
        FAIL=$((FAIL + 1))
    fi
}

# --- Test 1: Basic echo ---
echo "Test 1: Basic echo"
RESP=$(curl -s -X POST http://localhost:18080/request \
    -H 'Content-Type: application/json' \
    -d '{"method":"greet","payload":{"msg":"hello"}}')
check "response contains echo" '"echo"' "$RESP"
check "response contains hello" '"hello"' "$RESP"

# --- Test 2: Concurrent requests ---
echo "Test 2: Concurrent requests (10 parallel)"
PIDS=()
TMPDIR=$(mktemp -d)
for i in $(seq 1 10); do
    curl -s -X POST http://localhost:18080/request \
        -H 'Content-Type: application/json' \
        -d "{\"method\":\"op\",\"payload\":{\"i\":$i}}" \
        -o "$TMPDIR/resp_$i" &
    PIDS+=($!)
done

ALL_OK=true
for pid in "${PIDS[@]}"; do
    if ! wait "$pid"; then
        ALL_OK=false
    fi
done

if $ALL_OK; then
    GOT_ALL=true
    for i in $(seq 1 10); do
        if ! grep -q "\"i\":$i" "$TMPDIR/resp_$i" 2>/dev/null; then
            GOT_ALL=false
            break
        fi
    done
    if $GOT_ALL; then
        echo "  PASS: all 10 concurrent responses matched"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: some concurrent responses did not match"
        FAIL=$((FAIL + 1))
    fi
else
    echo "  FAIL: some curl processes failed"
    FAIL=$((FAIL + 1))
fi
rm -rf "$TMPDIR"

# --- Summary ---
echo ""
echo "Results: $PASS passed, $FAIL failed"
if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
echo "PASS"
