set -euo pipefail

rm -rf dashboard-build dist build

# Copy dashboard files
mkdir -p dashboard-build
cp -r waphl-data/src/ waphl-data/dashboard/. dashboard-build/
cat waphl-data/pyproject.toml \
  | grep -Ev 'firecloud|google' \
  | sed 's/requires = \["setuptools[^"]*"\]/requires = ["setuptools"]/' \
  > dashboard-build/pyproject.toml

# Resolve a real Python binary from /opt first, then fall back to PATH
PYTHON_BIN="$(
  find /opt -type f -path '*/bin/python*' -executable 2>/dev/null \
    | grep -E '/bin/python([0-9]+(\.[0-9]+)?)?$' \
    | sort -V \
    | tail -n 1
)"

if [ -z "${PYTHON_BIN}" ]; then
  PYTHON_BIN="$(command -v python3 || true)"
fi

if [ -z "${PYTHON_BIN}" ]; then
  PYTHON_BIN="$(command -v python || true)"
fi

if [ -z "${PYTHON_BIN}" ]; then
  echo "Error: could not find a usable Python executable under /opt or in PATH." >&2
  exit 1
fi

echo "Using Python: ${PYTHON_BIN}"
"${PYTHON_BIN}" --version

# Ensure pip is available
if ! "${PYTHON_BIN}" -m pip --version >/dev/null 2>&1; then
  echo "Error: pip is not available for ${PYTHON_BIN}" >&2
  exit 1
fi

# Ensure build is installed
if ! "${PYTHON_BIN}" -m build --version >/dev/null 2>&1; then
  echo "Installing Python package 'build'..."
  "${PYTHON_BIN}" -m pip install build
fi

# Build the package inside dashboard-build
(
  cd dashboard-build
  "${PYTHON_BIN}" -m build --wheel
)

# Point requirements.txt at the built wheel
WHEEL_PATH="$(find dashboard-build/dist -maxdepth 1 -type f -name '*.whl' | sort -V | tail -n 1)"

if [ -z "${WHEEL_PATH}" ]; then
  echo "Error: no wheel was built in dashboard-build/dist" >&2
  exit 1
fi

echo "./dist/$(basename "${WHEEL_PATH}")" > dashboard-build/requirements.txt

echo "Generated requirements.txt:"
cat dashboard-build/requirements.txt

# replace --new with --app-id if the guid is known and you want to redeploy the same app
rsconnect \
    deploy streamlit \
    --new \
    -t 'WAPHL Bioinformatics Results' \
    -E AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
    -E AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
    -E AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION}" \
    -E PROD_BUCKET="${PROD_BUCKET}" \
    -E RES_BUCKET="${RES_BUCKET}" \
    -E P2R_QUEUE_URL="${P2R_QUEUE_URL}" \
    -E TERRA_PROJECT="${TERRA_PROJECT}" \
    -E GCS_CREDENTIALS_URI="${GCS_CREDENTIALS_URI}" \
    -E P2RT_JOB_QUEUE="${P2RT_JOB_QUEUE}" \
    -E P2RT_JOB_DEFINITION="${P2RT_JOB_DEFINITION}" \
    -E TERRA_WORKSPACES="${TERRA_WORKSPACES}" \
    dashboard-build/