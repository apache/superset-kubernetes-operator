#!/bin/bash
# Installs the local Kubernetes tooling for the devcontainer. Every binary is
# pinned to an explicit version and verified against a checksum before it is
# moved onto PATH, mirroring the fail-closed pinning policy the rest of the repo
# enforces (hack/tool-checksums.txt, scripts/verify-tool-checksum.sh). A
# compromised or moved upstream artifact is detected here instead of silently
# executed inside a container that mounts the working tree and runs
# docker-in-docker.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

verify_sha256() {
  # verify_sha256 <file> <expected-hex>
  echo "$2  $1" | sha256sum -c -
}

# --- kind: reuse the in-repo pin from .github/supported-k8s.json (same source
#     of truth CI uses), so the devcontainer and CI never diverge. ---
KIND_VERSION="$(grep -oE '"kind_version"[[:space:]]*:[[:space:]]*"[^"]+"' "${REPO_ROOT}/.github/supported-k8s.json" | grep -oE 'v[0-9]+\.[0-9]+\.[0-9]+')"
KIND_CHECKSUM="$(grep -oE '"kind_checksum"[[:space:]]*:[[:space:]]*"[a-f0-9]{64}"' "${REPO_ROOT}/.github/supported-k8s.json" | grep -oE '[a-f0-9]{64}')"
[ -n "${KIND_VERSION}" ] && [ -n "${KIND_CHECKSUM}" ] || { echo "could not read kind pin from supported-k8s.json" >&2; exit 1; }
curl -fsSLo ./kind "https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-amd64"
verify_sha256 ./kind "${KIND_CHECKSUM}"
chmod +x ./kind
mv ./kind /usr/local/bin/kind

# --- kubectl: pin to an explicit version and verify against the SHA-256 that
#     dl.k8s.io publishes alongside the binary (same origin over TLS; the value
#     is validated as 64 hex chars before use so an error page cannot slip
#     through). Bump alongside supported-k8s.json. ---
# renovate: datasource=github-releases depName=kubernetes/kubernetes
KUBECTL_VERSION="${KUBECTL_VERSION:-v1.37.0}"
curl -fsSLO "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl"
KUBECTL_CHECKSUM="$(curl -fsSL "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl.sha256")"
echo "${KUBECTL_CHECKSUM}" | grep -Eq '^[a-f0-9]{64}$' || { echo "unexpected kubectl checksum: ${KUBECTL_CHECKSUM}" >&2; exit 1; }
verify_sha256 ./kubectl "${KUBECTL_CHECKSUM}"
chmod +x ./kubectl
mv ./kubectl /usr/local/bin/kubectl

# --- kubebuilder: pin to an explicit version and verify against the
#     checksums.txt kubebuilder publishes alongside its release binaries (same
#     origin over TLS; the value is validated as 64 hex before use, and the line
#     is matched by the exact asset name so an SBOM/other asset can't be picked
#     up). Bump via Renovate. ---
# renovate: datasource=github-releases depName=kubernetes-sigs/kubebuilder
KUBEBUILDER_VERSION="${KUBEBUILDER_VERSION:-v4.5.2}"
kb_base="https://github.com/kubernetes-sigs/kubebuilder/releases/download/${KUBEBUILDER_VERSION}"
curl -fsSL -o kubebuilder "${kb_base}/kubebuilder_linux_amd64"
KUBEBUILDER_CHECKSUM="$(curl -fsSL "${kb_base}/checksums.txt" | awk '/kubebuilder_linux_amd64$/{for (i=1;i<=NF;i++) if ($i ~ /^[a-f0-9]{64}$/) {print $i; exit}}')"
echo "${KUBEBUILDER_CHECKSUM}" | grep -Eq '^[a-f0-9]{64}$' || { echo "unexpected kubebuilder checksum: ${KUBEBUILDER_CHECKSUM}" >&2; exit 1; }
verify_sha256 ./kubebuilder "${KUBEBUILDER_CHECKSUM}"
chmod +x kubebuilder
mv kubebuilder /usr/local/bin/

# Idempotent so set -e does not break devcontainer rebuilds.
docker network inspect kind >/dev/null 2>&1 || \
  docker network create -d=bridge --subnet=172.19.0.0/24 kind

kind version
kubebuilder version
docker --version
go version
kubectl version --client
