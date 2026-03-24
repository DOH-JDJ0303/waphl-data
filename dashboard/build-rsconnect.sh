rm -rf dashboard-build

# Copy dashboard files
mkdir -p dashboard-build
cp -r waphl-data/dashboard/. dashboard-build/

# Install waphl-data as a package into the build directory
pip install waphl-data/ --target dashboard-build/

rsconnect \
    deploy streamlit \
    -t 'WAPHL Bioinformatics Results' \
    -E AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
    -E AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
    -E AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION}" \
    -E PROD_BUCKET="${PROD_BUCKET}" \
    -E RES_BUCKET="${RES_BUCKET}" \
    -E P2R_QUEUE_URL="${P2R_QUEUE_URL}" \
    -E TERRA_PROJECT="${TERRA_PROJECT}" \
    -E GOOGLE_CLOUD_CREDENTIALS="${GCS_CREDENTIALS_URI}" \
    -E P2RT_JOB_QUEUE="${P2RT_JOB_QUEUE}" \
    -E P2RT_JOB_DEFINITION="${P2RT_JOB_DEFINITION}" \
    -E TERRA_WORKSPACES="${TERRA_WORKSPACES}" \
    dashboard-build/