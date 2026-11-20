#!/usr/bin/env bash
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
sib="${1:-$(dirname "$here")}"
rc=0

contract() { grep -v '^[[:space:]]*;' "$1" | sed -n 's/^[[:space:]]*\(version .*\)$/\1/p; s/^[[:space:]]*\(method [^)]*) -> [^ ]*\).*$/\1/p'; }

lez_copy="$here/rust-lib/deps/rln_lez_module.lidl"
lez_src="$sib/logos-rln-lez-module/rust-lib/rln_lez_module.lidl"
if [ -f "$lez_src" ]; then
  missing="$(comm -23 <(contract "$lez_copy" | sort) <(contract "$lez_src" | sort))"
  if [ -n "$missing" ]; then
    echo "rln_lez_module.lidl: not in the provider's contract:"; echo "$missing" | sed 's/^/  /'; rc=1
  else
    echo "rln_lez_module.lidl: ok"
  fi
else
  echo "rln_lez_module.lidl: no checkout at $lez_src"; rc=1
fi

cl_copy="$here/rust-lib/deps/membership_allocation_client_module.lidl"
cl_src="$sib/logos-membership-allocation-client-module/rust-lib/generated/membership_allocation_client_module.lidl"
if [ -f "$cl_src" ]; then
  if diff <(grep -v '^[[:space:]]*;' "$cl_copy") "$cl_src" >/dev/null; then
    echo "membership_allocation_client_module.lidl: ok"
  else
    echo "membership_allocation_client_module.lidl: differs from $cl_src"; rc=1
  fi
else
  echo "membership_allocation_client_module.lidl: no generated contract at $cl_src"; rc=1
fi
exit "$rc"
