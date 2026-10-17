#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
tmp_parent="${TMPDIR:-/tmp}"
tmp="$(mktemp -d "$tmp_parent/den-hearth-release-test.XXXXXX")"
cleanup() {
    case "$tmp" in
        "$tmp_parent"/den-hearth-release-test.*) rm -rf -- "$tmp" ;;
    esac
}
trap cleanup EXIT

remote="$tmp/remote.git"
repo="$tmp/repo"
fake_bin="$tmp/bin"
mkdir -p "$fake_bin"
git init --quiet --bare --initial-branch=main "$remote"
git init --quiet --initial-branch=main "$repo"
git -C "$repo" config user.name "release test"
git -C "$repo" config user.email "release-test@example.invalid"
mkdir -p "$repo/scripts"
cp "$root/scripts/release.sh" "$repo/scripts/release.sh"
printf '%s\n' 1.0.0 > "$repo/VERSION"
git -C "$repo" add VERSION scripts/release.sh
git -C "$repo" commit --quiet -m initial
git -C "$repo" remote add origin "$remote"
git -C "$repo" push --quiet --set-upstream origin main

cat > "$fake_bin/cargo" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
cat > "$fake_bin/gh" <<'EOF'
#!/usr/bin/env bash
if [ "${1:-} ${2:-}" = "pr list" ]; then
    exit 0
fi
if [ "${1:-} ${2:-}" = "pr create" ]; then
    printf '%s\n' "$*" > "$GH_LOG"
    printf '%s\n' 'https://example.invalid/pull/1'
    exit 0
fi
echo "unexpected gh invocation: $*" >&2
exit 1
EOF
chmod +x "$fake_bin/cargo" "$fake_bin/gh"

if output="$(cd "$repo" && PATH="$fake_bin:$PATH" scripts/release.sh 1.0.1 2>&1)"; then
    echo "release unexpectedly accepted an unprepared VERSION" >&2
    exit 1
fi
grep -Fq 'protected main requires a version pull request first' <<<"$output"
[ "$(git --git-dir="$remote" show main:VERSION)" = 1.0.0 ]
! git --git-dir="$remote" show-ref --verify --quiet refs/heads/release/1.0.1
! git --git-dir="$remote" show-ref --tags --quiet

(cd "$repo" && GH_LOG="$tmp/gh.log" PATH="$fake_bin:$PATH" scripts/release.sh --prepare 1.0.1)
[ "$(git --git-dir="$remote" show main:VERSION)" = 1.0.0 ]
[ "$(git --git-dir="$remote" show release/1.0.1:VERSION)" = 1.0.1 ]
! git --git-dir="$remote" show-ref --tags --quiet
grep -Fq 'pr create' "$tmp/gh.log"
grep -Fq -- '--base main --head release/1.0.1 --title Release 1.0.1' "$tmp/gh.log"

echo "release preparation test passed"
