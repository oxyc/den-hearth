#!/usr/bin/env bash
set -euo pipefail

case ${1:?usage: release-metadata.sh version|bake-digest|bake-digests} in
  version)
    version=$(cat VERSION)
    [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || {
      echo "::error::VERSION is not X.Y.Z: $version" >&2
      exit 1
    }
    echo "version=$version"
    ;;
  bake-digest)
    digest=$(jq -r '.hearth["containerimage.digest"] // empty' <<<"${METADATA:?METADATA is required}")
    [[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || {
      echo "::error::no image digest in the build metadata" >&2
      exit 1
    }
    echo "digest=$digest"
    ;;
  bake-digests)
    hearth=$(jq -r '.hearth["containerimage.digest"] // empty' <<<"${METADATA:?METADATA is required}")
    controller=$(jq -r '.controller["containerimage.digest"] // empty' <<<"$METADATA")
    [[ "$hearth" =~ ^sha256:[0-9a-f]{64}$ ]] || {
      echo "::error::no Hearth image digest in the build metadata" >&2
      exit 1
    }
    [[ "$controller" =~ ^sha256:[0-9a-f]{64}$ ]] || {
      echo "::error::no controller image digest in the build metadata" >&2
      exit 1
    }
    printf 'digest=%s\ncontroller_digest=%s\n' "$hearth" "$controller"
    ;;
  *)
    echo "unknown metadata operation: $1" >&2
    exit 2
    ;;
esac
