gcloud dataproc clusters create phs_cluster \
    --project=YOUR_PROJECT_ID \
    --region=asia-southeast1 \
    --single-node \
    --enable-component-gateway \
    # --optional-components=COMPONENT \
    # --properties=PROPERTIES