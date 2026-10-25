# The two builds CI runs from the Dockerfile. Each writes its own GitHub Actions cache scope, so the e2e job and
# the image build never overwrite each other's cache when they run side by side; the e2e build also reads the
# image's scope for the ffmpeg stage they share.
group "default" {
  targets = ["hearth"]
}

target "controller" {
  context    = "."
  dockerfile = "Dockerfile.controller"
  platforms  = ["linux/amd64", "linux/arm64"]
  pull       = true
  attest = [
    "type=provenance,mode=max",
    "type=sbom",
  ]
}

target "common" {
  context    = "."
  dockerfile = "Dockerfile"
  platforms  = ["linux/amd64"]
}

# The runtime image. DEN_VERSION and GIT_REVISION come from the workflow (`*.args.…`).
target "hearth" {
  inherits = ["common"]
  target   = "runtime"
  # Re-pull every FROM: a base image served from the build cache is how a release inherits last month's libc.
  pull       = true
  cache-from = ["type=gha,scope=release-image"]
  # SLSA provenance and an SBOM, attached to the image index and so covered by its signature.
  attest = [
    "type=provenance,mode=max",
    "type=sbom",
  ]
}

# Remux's #[ignore]d end-to-end tests against the image's own ffmpeg. Nothing is exported but the cache.
target "e2e" {
  inherits   = ["common"]
  target     = "e2e"
  cache-from = ["type=gha,scope=e2e", "type=gha,scope=release-image"]
  cache-to   = ["type=gha,mode=max,scope=e2e"]
  output     = ["type=cacheonly"]
}
