#!/usr/bin/env bash
# Tree-keyed CI verdicts. ci.yml uploads an artifact named `ci-verdict-<source tree>` only after every blocking test
# job passed on that tree, so a later run on byte-identical source can reuse the result instead of testing it again.
# The source tree is the commit's tree without VERSION: release.sh's version commit changes nothing else, and the
# image reads the version at run time (the candidate is still built, probed and scanned for every main SHA).
#
#   scripts/ci-verdict.sh tree <rev>           print the source tree of <rev>
#   scripts/ci-verdict.sh find <owner/repo> <tree>
#                                              print the id of a ci.yml run in this repository (not a fork) that
#                                              recorded a pass for <tree>; print nothing when there is none
set -euo pipefail

case "${1:-}" in
    tree)
        # Listed first so an unknown revision fails here instead of printing the empty tree.
        listing=$(git ls-tree "$2")
        grep -v $'\tVERSION$' <<<"$listing" | git mktree
        ;;
    find)
        repo=$2 tree=$3
        # A fork's pull_request run can upload artifacts under any name, so only same-repository runs count.
        ids=$(gh api "repos/$repo/actions/artifacts?name=ci-verdict-$tree&per_page=100" --jq '.artifacts[]
            | select(.expired | not)
            | select(.workflow_run.head_repository_id == .workflow_run.repository_id)
            | .workflow_run.id')
        for id in $ids; do
            if [ "$(gh api "repos/$repo/actions/runs/$id" --jq .path)" = .github/workflows/ci.yml ]; then
                echo "$id"
                exit 0
            fi
        done
        ;;
    *)
        echo "usage: scripts/ci-verdict.sh tree <rev> | find <owner/repo> <tree>" >&2
        exit 2
        ;;
esac
