#!/usr/bin/env bash
# Runs every automated gate and prints PASS / FAIL / NOT RUN.
# Compatible with macOS bash 3.2 (no associative arrays).
# A gate that cannot run counts as failure unless ALLOW_SKIP=1.
set -u
SUMMARY=""; FAIL=0; SKIP=0

add() { SUMMARY="${SUMMARY}$(printf '%-12s %s' "$1" "$2")"$'\n'; }
skip() { add "$1" "NOT RUN ($2)"; SKIP=1; }
run() {
  local name="$1"; shift
  if ! command -v "$1" >/dev/null 2>&1; then skip "$name" "$1 not installed — run: make tools"; return; fi
  echo "==> $name: $*"
  if "$@"; then add "$name" "PASS"; else add "$name" "FAIL"; FAIL=1; fi
}

if [ -f go.mod ]; then
  run build   go build ./...
  run test    go test -race -cover ./...
  if [ -n "${TEST_DB_DSN:-}" ]; then
    run integration go test -race -count=1 -tags=integration ./...
  else
    skip integration "TEST_DB_DSN not set — start Docker, then: make verify"
  fi
  run lint    golangci-lint run
  run gosec   gosec -quiet ./...
  run vulns   govulncheck ./...
else
  skip go "no go.mod — run: go mod init <module>"
fi

if git rev-parse --git-dir >/dev/null 2>&1; then
  run secrets gitleaks git --no-banner --redact
else
  run secrets gitleaks dir . --no-banner --redact
fi

if [ -f contracts/openapi.yaml ]; then
  if [ -x node_modules/.bin/redocly ]; then run contract node_modules/.bin/redocly lint contracts/openapi.yaml
  else skip contract "run: npm i -D @redocly/cli"; fi
fi

echo; echo "===== VERIFY SUMMARY ====="; printf '%s' "$SUMMARY"
if [ "$FAIL" = 1 ]; then echo "RESULT: FAIL"; exit 1; fi
if [ "$SKIP" = 1 ] && [ "${ALLOW_SKIP:-0}" != 1 ]; then echo "RESULT: INCOMPLETE (gates not run). Set ALLOW_SKIP=1 only with human approval."; exit 2; fi
echo "RESULT: PASS"
