set -e

rm -rf dashboard-build

# Copy dashboard files
mkdir -p dashboard-build
cp -r waphl-data/src/ waphl-data/dashboard/. dashboard-build/
cat waphl-data/pyproject.toml | grep -Ev 'firecloud|google' | sed 's/requires = \["setuptools[^"]*"\]/requires = ["setuptools"]/' > dashboard-build/pyproject.toml
echo '.' >> dashboard-build/requirements.txt

# Common environment flags
ENV_FLAGS=(
    -E AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID"
    -E AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY"
    -E AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION}"
    -E PROD_BUCKET="${PROD_BUCKET}"
    -E RES_BUCKET="${RES_BUCKET}"
    -E P2R_QUEUE_URL="${P2R_QUEUE_URL}"
    -E TERRA_PROJECT="${TERRA_PROJECT}"
    -E GOOGLE_CLOUD_CREDENTIALS="${GCS_CREDENTIALS_URI}"
    -E P2RT_JOB_QUEUE="${P2RT_JOB_QUEUE}"
    -E P2RT_JOB_DEFINITION="${P2RT_JOB_DEFINITION}"
    -E TERRA_WORKSPACES="${TERRA_WORKSPACES}"
)

# Check for existing deployment
GUID=$(rsconnect content search --name 'WAPHL Bioinformatics Results' 2>/dev/null \
    | python3 -c "import sys, json; items=json.load(sys.stdin); print(items[0]['guid'] if items else '')" 2>/dev/null || true)

if [ -n "$GUID" ]; then
    echo "Found existing deployment (GUID: $GUID) — redeploying..."
    rsconnect redeploy streamlit \
        --guid "$GUID" \
        "${ENV_FLAGS[@]}" \
        dashboard-build/
else
    echo "No existing deployment found — deploying for the first time..."
    rsconnect deploy streamlit \
        -t 'WAPHL Bioinformatics Results' \
        "${ENV_FLAGS[@]}" \
        dashboard-build/
fi