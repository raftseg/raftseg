#!/bin/sh
# Compares internal/testutil with testify in a throwaway module, so that
# testify never appears in raftseg's go.mod. Needs network or a module cache
# holding testify v1.10.0 and its dependencies.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/tu"
cp "$here/../../check.go" "$tmp/tu/check.go"
cp "$here/main.go.txt" "$tmp/main.go"
cd "$tmp"
go mod init tucmp >/dev/null 2>&1
go get github.com/stretchr/testify@v1.10.0 >/dev/null
go run .
