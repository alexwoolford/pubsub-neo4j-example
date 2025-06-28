# 🏥 Healthcare Pub/Sub to Neo4j Streaming Example

**Real-time healthcare data streaming from Google Cloud Pub/Sub to Neo4j with relationship modeling**

This example demonstrates real-time ingestion of connected healthcare data (patients, doctors, diagnoses, medications, procedures) into a Neo4j graph database via Google Cloud Pub/Sub, designed for healthcare analytics and clinical decision support.

## 🎯 **Key Features**

- **Healthcare Domain Models**: Realistic clinical data with ICD-10 codes, CPT codes, and medical relationships
- **High-Performance Streaming**: Up to 2,360+ messages/second throughput with real-time processing
- **Graph Relationships**: Creates meaningful clinical relationships automatically  
- **Production Architecture**: Cloud Run + Pub/Sub + Neo4j with auto-scaling
- **Performance Monitoring**: Real-time metrics and scaling recommendations
- **Complete Automation**: One-command setup, demo, and cleanup

## 🏗️ **Architecture**

```
Healthcare Data → Google Pub/Sub → Cloud Run App → Neo4j Database
```

**Components:**
- **Data Generator**: Produces realistic clinical data with proper relationships
- **Google Cloud Pub/Sub**: Reliable message queue for healthcare data streams
- **Healthcare Processor**: Flask app that creates graph relationships
- **Neo4j Database**: Graph database optimized for healthcare analytics

## 📊 **Healthcare Data Model**

### Node Types
- **Hospital**: Healthcare facilities with location and capacity data
- **Doctor**: Medical practitioners with specialties and affiliations
- **Patient**: Individuals with medical records and demographics
- **Diagnosis**: Medical conditions with ICD-10 codes and severity
- **Medication**: Prescribed drugs with dosages and frequencies
- **Procedure**: Medical procedures with CPT codes and costs

### Relationships
- `(Doctor)-[:WORKS_AT]->(Hospital)`
- `(Patient)-[:HAS_PRIMARY_CARE_DOCTOR]->(Doctor)`
- `(Patient)-[:HAS_DIAGNOSIS]->(Diagnosis)`
- `(Doctor)-[:DIAGNOSED]->(Diagnosis)`
- `(Patient)-[:PRESCRIBED]->(Medication)`
- `(Doctor)-[:PRESCRIBED]->(Medication)`
- `(Patient)-[:UNDERWENT]->(Procedure)`
- `(Doctor)-[:PERFORMED]->(Procedure)`
- `(Procedure)-[:PERFORMED_AT]->(Hospital)`

## 🚀 **Quick Start**

### Prerequisites
- Google Cloud Project with billing enabled
- Neo4j Aura account (free tier available)
- Python 3.9+

### 1. **Setup**
```bash
git clone <this-repo>
cd healthcare-streaming-demo

# Install dependencies
pip install -r requirements.txt

# Copy environment template
cp env.example .env
# Edit .env with your Neo4j and Google Cloud credentials
```

### 2. **Run Complete Demo**
```bash
# Automated demo with infrastructure provisioning and cleanup
python demo_automation.py --project-id your-project-id --demo-type local

# Or manual demo
python healthcare_app.py                    # Start processor
python healthcare_publisher.py --project-id your-project --mode medium
```

### 3. **Monitor Performance**
```bash
curl http://localhost:8080/health           # Health and performance metrics
curl http://localhost:8080/stats            # Detailed statistics
curl http://localhost:8080/graph-sample     # Sample relationships
```

## 📈 **Performance Results**

| Dataset Size | Messages | Throughput | Assessment | Use Case |
|--------------|----------|------------|------------|----------|
| Small        | 775      | 441 msg/sec | MODERATE | Regional clinic daily volume |
| Medium       | 2,155    | 897 msg/sec | GOOD | Hospital system daily volume |
| Large        | 7,605    | 1,386 msg/sec | EXCELLENT | Major medical center daily volume |
| Massive      | 38,005   | 2,360 msg/sec | EXCELLENT | Enterprise healthcare network |

*Performance scales significantly with dataset size. All tests show 100% success rate with automatic relationship creation.*

## 🏥 **Healthcare Use Cases**

1. **Patient Journey Tracking**: Follow patients through diagnoses, treatments, procedures
2. **Provider Networks**: Map relationships between doctors, hospitals, specialties
3. **Medication Management**: Track prescriptions, dosages, patient adherence
4. **Clinical Decision Support**: Graph-based recommendations and insights

## 💰 **Cost Analysis**

- **Development/Demo**: $0 (Free tiers + automatic cleanup)
- **Production (1M messages/month)**: ~$15-25
  - Google Cloud Run: $10-15 (scales to zero)
  - Google Pub/Sub: $3-5
  - Neo4j Aura: Free tier or $65+ for production

## 🔍 **Sample Neo4j Queries**

### Find Patients with Multiple Conditions
```cypher
MATCH (p:Patient)-[:HAS_DIAGNOSIS]->(d:Diagnosis)
WITH p, count(d) as diagnosis_count
WHERE diagnosis_count > 2
RETURN p.name, p.age, diagnosis_count
ORDER BY diagnosis_count DESC
```

### Analyze Doctor Workload
```cypher
MATCH (d:Doctor)-[:DIAGNOSED]->(diag:Diagnosis)
MATCH (d)-[:PRESCRIBED]->(med:Medication)
WITH d, count(diag) as diagnoses, count(med) as prescriptions
RETURN d.name, d.specialty, diagnoses, prescriptions
ORDER BY (diagnoses + prescriptions) DESC
```

## 🚀 **Cloud Deployment**

### Local Development
```bash
python healthcare_app.py                    # Runs locally with pull subscription
```

### Production Deployment
```bash
python demo_automation.py --project-id your-project --demo-type cloud
# Automatically builds container, deploys to Cloud Run, configures Pub/Sub
```

## 📊 **Monitoring & Observability**

- `GET /health` - Service health with performance metrics
- `GET /stats` - Comprehensive statistics  
- `GET /metrics/real-time` - Real-time performance monitoring
- Structured JSON logging for all events
- Automatic scaling recommendations

## 🧹 **Cleanup**

```bash
# Automated cleanup
python demo_automation.py --project-id your-project --cleanup-only

# Manual cleanup
gcloud run services delete healthcare-processor
gcloud pubsub subscriptions delete neo4j-subscription
gcloud pubsub topics delete neo4j-topic
```

## 🛠️ **Development**

### Project Structure
```
healthcare-streaming-demo/
├── healthcare_app.py              # Main Flask application
├── healthcare_neo4j_service.py    # Neo4j service with relationships
├── healthcare_data_generator.py   # Clinical data generator
├── healthcare_publisher.py        # High-performance publisher
├── demo_automation.py             # Complete automation script
├── requirements.txt               # Dependencies
├── Dockerfile                     # Container configuration
└── README.md                      # This file
```

### Running Tests
```bash
# Test data generation
python healthcare_data_generator.py

# Test different dataset sizes
python healthcare_publisher.py --project-id your-project --mode small
python healthcare_publisher.py --project-id your-project --mode large

# Custom dataset
python healthcare_publisher.py --project-id your-project \
  --doctors 100 --patients 500 --diagnoses 1000
```

## 📄 **License**

MIT License - feel free to use this for your healthcare analytics projects.

## 🤝 **Contributing**

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## 📞 **Support**

For questions or deployment assistance, refer to:
- [Neo4j Documentation](https://neo4j.com/docs/)
- [Google Cloud Pub/Sub Documentation](https://cloud.google.com/pubsub/docs)
- [Google Cloud Run Documentation](https://cloud.google.com/run/docs)

---

**🏥 Demonstrating scalable graph-based healthcare data processing with real-time streaming** 