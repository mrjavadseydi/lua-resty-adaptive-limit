#!/usr/bin/env bash
# Release consistency check.
#
#   scripts/check-release.sh 0.1.1           # verify the tree is ready to tag v0.1.1
#   scripts/check-release.sh --notes 0.1.1   # print that version's CHANGELOG section
#
# Run by the release workflow before anything is published, and locally via
# `make release-check VERSION=x.y.z`. Portable to macOS bash 3.2 / BSD tools.
set -eu

cd "$(dirname "$0")/.."

PKG=lua-resty-adaptive-limit
MAIN=lib/resty/adaptive_limit.lua

notes() {
    # Body of "## [VERSION] ..." up to the next "## [" heading or the link
    # reference block; leading/trailing blank lines trimmed.
    awk -v v="$1" '
        index($0, "## [" v "]") == 1 { on = 1; next }
        on && (/^## \[/ || /^\[[^]]+\]: /) { exit }
        on { buf[++n] = $0 }
        END {
            s = 1; while (s <= n && buf[s] == "") s++
            e = n; while (e >= s && buf[e] == "") e--
            for (i = s; i <= e; i++) print buf[i]
        }' CHANGELOG.md
}

if [ "${1:-}" = "--notes" ]; then
    [ -n "${2:-}" ] || { echo "usage: $0 --notes VERSION" >&2; exit 2; }
    out=$(notes "$2")
    [ -n "$out" ] || { echo "no CHANGELOG section for $2" >&2; exit 1; }
    printf '%s\n' "$out"
    exit 0
fi

VERSION=${1:-}
VERSION=${VERSION#v}
if ! printf '%s' "$VERSION" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$'; then
    echo "usage: $0 VERSION   (x.y.z, optionally prefixed with v)" >&2
    exit 2
fi

fail=0
ok()  { printf '  ok    %s\n' "$1"; }
bad() { printf '  FAIL  %s\n' "$1"; fail=1; }

echo "release check for $PKG $VERSION"

# 1. The Lua module is the single source of truth (opm reads it too).
code_ver=$(sed -n 's/^[[:space:]]*_VERSION[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' "$MAIN" | head -n 1)
if [ "$code_ver" = "$VERSION" ]; then ok "$MAIN _VERSION = $code_ver"
else bad "$MAIN _VERSION is '$code_ver', expected '$VERSION'"; fi

# 2. Release rockspec: name, version, tag.
ROCK="$PKG-$VERSION-1.rockspec"
DEV="$PKG-dev-1.rockspec"
if [ -f "$ROCK" ]; then
    grep -q "^version = \"$VERSION-1\"" "$ROCK" \
        && ok "$ROCK version" || bad "$ROCK: version is not \"$VERSION-1\""
    grep -Eq "^[[:space:]]*tag = \"v$VERSION\"" "$ROCK" \
        && ok "$ROCK source tag v$VERSION" || bad "$ROCK: source tag is not \"v$VERSION\""
else
    bad "missing $ROCK"
fi
for f in "$PKG"-*.rockspec; do
    case "$f" in "$ROCK"|"$DEV") ;; *) bad "stale rockspec at repo root: $f" ;; esac
done
[ -f "$DEV" ] && ok "$DEV present" || bad "missing $DEV"

# 3. Every lib/ file is shipped, each under the module name its path implies,
#    and the dev rockspec ships the same set.
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
find lib -name '*.lua' | LC_ALL=C sort > "$tmp/files"
modules() {
    sed -n 's/^[[:space:]]*\["\([^"]*\)"\][[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1 \2/p' "$1"
}
for spec in "$ROCK" "$DEV"; do
    [ -f "$spec" ] || continue
    modules "$spec" > "$tmp/pairs"
    awk '{ print $2 }' "$tmp/pairs" | LC_ALL=C sort > "$tmp/listed"
    missing=$(LC_ALL=C comm -23 "$tmp/files" "$tmp/listed")
    extra=$(LC_ALL=C comm -13 "$tmp/files" "$tmp/listed")
    [ -z "$missing" ] || bad "$spec does not ship: $(echo $missing)"
    [ -z "$extra" ]   || bad "$spec lists files that do not exist: $(echo $extra)"
    misnamed=$(awk '{
        p = $2; sub(/^lib\//, "", p); sub(/\.lua$/, "", p); gsub(/\//, ".", p)
        if (p != $1) print $1 "->" $2 }' "$tmp/pairs")
    [ -z "$misnamed" ] || bad "$spec module/path mismatch: $(echo $misnamed)"
    [ -z "$missing$extra$misnamed" ] && ok "$spec ships all $(wc -l < "$tmp/files" | tr -d ' ') modules"
done

# 4. OPM metadata: version must come from main_module, never be pinned.
if [ -f dist.ini ]; then
    grep -q "^main_module = $MAIN\$" dist.ini \
        && ok "dist.ini main_module" || bad "dist.ini: main_module is not $MAIN"
    if grep -Eq '^[[:space:]]*version[[:space:]]*=' dist.ini; then
        bad "dist.ini pins a version; let opm read _VERSION from $MAIN"
    fi
else
    bad "missing dist.ini"
fi

# 5. Documentation.
grep -q "^## \[$VERSION\] — [0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}" CHANGELOG.md \
    && ok "CHANGELOG has a dated [$VERSION] section" \
    || bad "CHANGELOG: no '## [$VERSION] — YYYY-MM-DD' heading"
[ -n "$(notes "$VERSION")" ] || bad "CHANGELOG: [$VERSION] section is empty"
grep -q "^\[$VERSION\]: " CHANGELOG.md \
    && ok "CHANGELOG has a [$VERSION] link" || bad "CHANGELOG: no [$VERSION] link reference"
grep -q "^Status: \*\*$VERSION\*\*" README.md \
    && ok "README status line" || bad "README: status line does not say **$VERSION**"

if [ "$fail" -ne 0 ]; then
    echo "release check FAILED"
    exit 1
fi
echo "release check passed"
