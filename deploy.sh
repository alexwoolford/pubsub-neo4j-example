#!/bin/bash

# Google Cloud Run deployment script for Pub/Sub to Neo4j processor

set -e  # Exit on any error

# Configuration
PROJECT_ID=${1:-"your-project-id"}
SERVICE_NAME="pubsub-neo4j-processor"
REGION=${2:-"us-central1"}
IMAGE_NAME="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"
TOPIC_NAME="neo4j-topic"
SUBSCRIPTION_NAME="neo4j-subscription"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}  Deploying Pub/Sub to Neo4j Processor${NC}"
echo -e "${BLUE}============================================${NC}"
echo -e "Project ID: ${PROJECT_ID}"
echo -e "Service Name: ${SERVICE_NAME}"
echo -e "Region: ${REGION}"
echo -e "Image: ${IMAGE_NAME}"
echo -e "${BLUE}============================================${NC}"

# Check if project ID was provided
if [ "$PROJECT_ID" = "your-project-id" ]; then
    echo -e "${RED}Error: Please provide your Google Cloud Project ID${NC}"
    echo "Usage: $0 <PROJECT_ID> [REGION]"
    echo "Example: $0 my-project-123 us-central1"
    exit 1
fi

# Check if required tools are installed
command -v gcloud >/dev/null 2>&1 || { echo -e "${RED}Error: gcloud CLI is required but not installed.${NC}" >&2; exit 1; }
command -v docker >/dev/null 2>&1 || { echo -e "${RED}Error: Docker is required but not installed.${NC}" >&2; exit 1; }

echo -e "${YELLOW}Step 1: Setting up Google Cloud configuration...${NC}"
gcloud config set project $PROJECT_ID
gcloud config set run/region $REGION

echo -e "${YELLOW}Step 2: Enabling required APIs...${NC}"
gcloud services enable \
    cloudbuild.googleapis.com \
    run.googleapis.com \
    pubsub.googleapis.com \
    containerregistry.googleapis.com

echo -e "${YELLOW}Step 3: Building and pushing Docker image...${NC}"
# Build the image using Cloud Build (recommended for Cloud Run)
gcloud builds submit --tag $IMAGE_NAME .

echo -e "${YELLOW}Step 4: Deploying to Cloud Run...${NC}"

# Get Neo4j credentials (you'll need to set these as environment variables or update the script)
if [ -z "$NEO4J_URI" ] || [ -z "$NEO4J_USERNAME" ] || [ -z "$NEO4J_PASSWORD" ]; then
    echo -e "${YELLOW}Warning: Neo4j credentials not found in environment variables.${NC}"
    echo -e "${YELLOW}Please set NEO4J_URI, NEO4J_USERNAME, and NEO4J_PASSWORD${NC}"
    echo -e "${YELLOW}Using placeholder values for now...${NC}"
    NEO4J_URI="bolt://your-neo4j-host:7687"
    NEO4J_USERNAME="neo4j"
    NEO4J_PASSWORD="your-neo4j-password"
fi

# Deploy to Cloud Run
gcloud run deploy $SERVICE_NAME \
    --image $IMAGE_NAME \
    --platform managed \
    --region $REGION \
    --allow-unauthenticated \
    --set-env-vars="NEO4J_URI=$NEO4J_URI,NEO4J_USERNAME=$NEO4J_USERNAME,NEO4J_PASSWORD=$NEO4J_PASSWORD,GOOGLE_CLOUD_PROJECT=$PROJECT_ID,PUBSUB_TOPIC=$TOPIC_NAME,PUBSUB_SUBSCRIPTION=$SUBSCRIPTION_NAME" \
    --memory=1Gi \
    --cpu=1 \
    --concurrency=100 \
    --max-instances=10 \
    --timeout=300

echo -e "${YELLOW}Step 5: Getting Cloud Run service URL...${NC}"
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --region=$REGION --format='value(status.url)')
WEBHOOK_URL="${SERVICE_URL}/webhook"

echo -e "${GREEN}Service deployed successfully!${NC}"
echo -e "Service URL: ${SERVICE_URL}"
echo -e "Webhook URL: ${WEBHOOK_URL}"

echo -e "${YELLOW}Step 6: Setting up Pub/Sub topic and subscription...${NC}"

# Create topic and subscription
python3 setup_pubsub.py \
    --project-id $PROJECT_ID \
    --topic $TOPIC_NAME \
    --subscription $SUBSCRIPTION_NAME \
    --push-endpoint $WEBHOOK_URL

echo -e "${YELLOW}Step 7: Testing the deployment...${NC}"

# Test health endpoint
echo "Testing health endpoint..."
curl -s "${SERVICE_URL}/health" | jq . || echo "Health check response received"

# Test with a sample message
echo -e "\nTesting direct message processing..."
curl -X POST "${SERVICE_URL}/process" \
    -H "Content-Type: application/json" \
    -d '{"type": "test", "message": "Deployment test", "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)'"}' \
    | jq . || echo "Direct processing test completed"

echo -e "\n${GREEN}============================================${NC}"
echo -e "${GREEN}  DEPLOYMENT COMPLETED SUCCESSFULLY!${NC}"
echo -e "${GREEN}============================================${NC}"
echo -e "Service URL: ${SERVICE_URL}"
echo -e "Webhook URL: ${WEBHOOK_URL}"
echo -e "Topic: ${TOPIC_NAME}"
echo -e "Subscription: ${SUBSCRIPTION_NAME}"
echo -e "\n${YELLOW}Next steps:${NC}"
echo -e "1. Update your Neo4j credentials if needed:"
echo -e "   gcloud run services update $SERVICE_NAME --region=$REGION \\"
echo -e "     --set-env-vars=\"NEO4J_URI=bolt://your-host:7687,NEO4J_USERNAME=neo4j,NEO4J_PASSWORD=your-password\""
echo -e "\n2. Test with sample messages:"
echo -e "   python3 publish_messages.py --project-id $PROJECT_ID --topic $TOPIC_NAME"
echo -e "\n3. Monitor logs:"
echo -e "   gcloud logs tail /projects/$PROJECT_ID/logs/run.googleapis.com%2Fstdout"
echo -e "${GREEN}============================================${NC}" 