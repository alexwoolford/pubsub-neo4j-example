#!/usr/bin/env python3
"""
Healthcare Pub/Sub to Neo4j Streaming Processor
High-performance healthcare data ingestion with relationship modeling
"""

import os
import json
import base64
import time
from flask import Flask, request, jsonify
from google.cloud import pubsub_v1
import structlog
from dotenv import load_dotenv
from healthcare_neo4j_service import HealthcareNeo4jService
from concurrent.futures import ThreadPoolExecutor
import signal
import sys
from datetime import datetime

# Load environment variables
load_dotenv()

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

app = Flask(__name__)

# Initialize Healthcare Neo4j service
neo4j_service = HealthcareNeo4jService(
    uri=os.getenv('NEO4J_URI'),
    username=os.getenv('NEO4J_USERNAME'),
    password=os.getenv('NEO4J_PASSWORD'),
    database=os.getenv('NEO4J_DATABASE', 'neo4j')
)

# Thread pool for async processing
executor = ThreadPoolExecutor(max_workers=20)  # Increased for higher throughput

# Performance tracking
performance_stats = {
    "messages_processed": 0,
    "messages_failed": 0,
    "total_processing_time": 0.0,
    "relationships_created": 0,
    "start_time": time.time()
}

def process_healthcare_message(message_data):
    """Process a healthcare message with performance tracking"""
    start_time = time.time()
    
    try:
        # Handle different message formats (pull vs push subscription)
        if isinstance(message_data, str):
            data = json.loads(message_data)
        elif isinstance(message_data, bytes):
            # For pull subscriptions, data comes as raw bytes
            decoded_data = message_data.decode('utf-8')
            data = json.loads(decoded_data)
        else:
            # For push subscriptions, data is base64-encoded
            try:
                decoded_data = base64.b64decode(message_data).decode('utf-8')
                data = json.loads(decoded_data)
            except Exception:
                data = json.loads(message_data)
        
        logger.info("Processing healthcare message", 
                   entity_type=data.get('type', 'unknown'),
                   entity_id=data.get('id', 'unknown'))
        
        # Process with healthcare service
        result = neo4j_service.process_healthcare_message(data)
        
        # Update performance stats
        processing_time = time.time() - start_time
        performance_stats["messages_processed"] += 1
        performance_stats["total_processing_time"] += processing_time
        performance_stats["relationships_created"] += result.get("relationships_created", 0)
        
        logger.info("Successfully processed healthcare message", 
                   entity_id=result.get('entity_id'), 
                   entity_type=result.get('type'),
                   relationships_created=result.get("relationships_created", 0),
                   processing_time_ms=round(processing_time * 1000, 2))
        
        return {"status": "success", 
                "entity_id": result.get('entity_id'),
                "type": result.get('type'),
                "relationships_created": result.get("relationships_created", 0),
                "processing_time_ms": round(processing_time * 1000, 2)}
        
    except Exception as e:
        processing_time = time.time() - start_time
        performance_stats["messages_failed"] += 1
        performance_stats["total_processing_time"] += processing_time
        
        logger.error("Error processing healthcare message", 
                    error=str(e), 
                    processing_time_ms=round(processing_time * 1000, 2),
                    exc_info=True)
        raise


@app.route('/health', methods=['GET'])
def health_check():
    """Comprehensive health check with performance metrics"""
    try:
        # Test Neo4j connection
        neo4j_service.test_connection()
        
        # Calculate performance metrics
        uptime = time.time() - performance_stats["start_time"]
        avg_processing_time = (performance_stats["total_processing_time"] / 
                             max(performance_stats["messages_processed"], 1))
        throughput = performance_stats["messages_processed"] / max(uptime, 1)
        
        # Performance assessment
        performance_level = "EXCELLENT" if throughput >= 1000 else \
                          "GOOD" if throughput >= 500 else \
                          "ACCEPTABLE" if throughput >= 100 else \
                          "POOR" if throughput >= 50 else "CRITICAL"
        
        return jsonify({
            "status": "healthy", 
            "service": "healthcare-processor",
            "performance": {
                "uptime_seconds": round(uptime, 2),
                "messages_processed": performance_stats["messages_processed"],
                "messages_failed": performance_stats["messages_failed"],
                "relationships_created": performance_stats["relationships_created"],
                "avg_processing_time_ms": round(avg_processing_time * 1000, 2),
                "throughput_msg_per_sec": round(throughput, 2),
                "throughput_level": performance_level,
                "success_rate": round(
                    (performance_stats["messages_processed"] / 
                     max(performance_stats["messages_processed"] + performance_stats["messages_failed"], 1)) * 100, 2
                ),
                "scaling_recommendation": get_scaling_recommendation(throughput, avg_processing_time * 1000)
            }
        }), 200
        
    except Exception as e:
        logger.error("Health check failed", error=str(e))
        return jsonify({"status": "unhealthy", "error": str(e)}), 503


@app.route('/webhook', methods=['POST'])
def pubsub_webhook():
    """Webhook endpoint for Pub/Sub push messages"""
    try:
        envelope = request.get_json()
        
        if not envelope:
            logger.warning("No Pub/Sub message received")
            return jsonify({"error": "No message received"}), 400
        
        # Extract message data
        message = envelope.get('message', {})
        data = message.get('data', '')
        attributes = message.get('attributes', {})
        
        logger.info("Received Pub/Sub push message", 
                   message_id=message.get('messageId'),
                   attributes=attributes)
        
        # Process the message asynchronously
        future = executor.submit(process_healthcare_message, data)
        result = future.result(timeout=30)
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error("Error processing webhook", error=str(e), exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/process', methods=['POST'])
def process_direct():
    """Direct processing endpoint for testing"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "No data provided"}), 400
        
        logger.info("Processing direct healthcare message", entity_type=data.get('type'))
        
        result = neo4j_service.process_healthcare_message(data)
        
        # Update performance stats for direct processing too
        performance_stats["messages_processed"] += 1
        performance_stats["relationships_created"] += result.get("relationships_created", 0)
        
        return jsonify({
            "status": "success", 
            "entity_id": result.get('entity_id'),
            "type": result.get('type'),
            "relationships_created": result.get("relationships_created", 0)
        }), 200
        
    except Exception as e:
        performance_stats["messages_failed"] += 1
        logger.error("Error processing direct message", error=str(e), exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/stats', methods=['GET'])
def get_statistics():
    """Get comprehensive healthcare statistics"""
    try:
        # Get Neo4j statistics
        healthcare_stats = neo4j_service.get_healthcare_statistics()
        
        # Get performance stats
        uptime = time.time() - performance_stats["start_time"]
        avg_processing_time = (performance_stats["total_processing_time"] / 
                             max(performance_stats["messages_processed"], 1))
        throughput = performance_stats["messages_processed"] / max(uptime, 1)
        
        return jsonify({
            "database_statistics": healthcare_stats,
            "performance_statistics": {
                "uptime_seconds": round(uptime, 2),
                "messages_processed": performance_stats["messages_processed"],
                "messages_failed": performance_stats["messages_failed"],
                "relationships_created": performance_stats["relationships_created"],
                "avg_processing_time_ms": round(avg_processing_time * 1000, 2),
                "throughput_msg_per_sec": round(throughput, 2),
                "success_rate": round(
                    (performance_stats["messages_processed"] / 
                     max(performance_stats["messages_processed"] + performance_stats["messages_failed"], 1)) * 100, 2
                )
            }
        }), 200
        
    except Exception as e:
        logger.error("Error getting statistics", error=str(e))
        return jsonify({"error": str(e)}), 500


@app.route('/graph-sample', methods=['GET'])
def get_graph_sample():
    """Get sample graph data for visualization"""
    try:
        limit = request.args.get('limit', 20, type=int)
        sample_data = neo4j_service.get_sample_graph_data(limit)
        
        return jsonify({
            "sample_relationships": sample_data,
            "count": len(sample_data)
        }), 200
        
    except Exception as e:
        logger.error("Error getting graph sample", error=str(e))
        return jsonify({"error": str(e)}), 500


def setup_pull_subscriber():
    """Set up pull subscriber for local development"""
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
    subscription_name = os.getenv('PUBSUB_SUBSCRIPTION')
    
    if not project_id or not subscription_name:
        logger.warning("Missing Pub/Sub configuration for pull subscriber")
        return None
    
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_name)
    
    def callback(message):
        try:
            logger.info("Received pull message", message_id=message.message_id)
            
            # Process the message
            future = executor.submit(process_healthcare_message, message.data)
            result = future.result(timeout=30)
            
            # Acknowledge the message
            message.ack()
            
            logger.info("Successfully processed pull message", result=result)
            
        except Exception as e:
            logger.error("Error processing pull message", error=str(e), exc_info=True)
            message.nack()
    
    return subscriber, subscription_path, callback


def get_scaling_recommendation(throughput: float, processing_time_ms: float) -> str:
    """Get scaling recommendation based on current performance"""
    if throughput < 50 or processing_time_ms > 1000:
        return "MIGRATE_TO_DATAFLOW_FOR_ENTERPRISE_SCALE"
    elif throughput < 100 or processing_time_ms > 500:
        return "INCREASE_CLOUD_RUN_INSTANCES_AND_OPTIMIZE"
    elif throughput < 500:
        return "OPTIMIZE_CONFIGURATION_OR_SCALE_HORIZONTALLY"
    else:
        return "CURRENT_ARCHITECTURE_PERFORMS_WELL"


@app.route('/metrics/real-time', methods=['GET'])
def real_time_metrics():
    """Real-time performance metrics endpoint"""
    try:
        uptime = time.time() - performance_stats["start_time"]
        current_throughput = performance_stats["messages_processed"] / max(uptime, 1)
        avg_processing_time = (performance_stats["total_processing_time"] / 
                             max(performance_stats["messages_processed"], 1)) * 1000
        
        # Get Neo4j statistics
        neo4j_stats = neo4j_service.get_healthcare_statistics()
        
        return jsonify({
            "timestamp": datetime.utcnow().isoformat(),
            "throughput": {
                "current_msg_per_sec": round(current_throughput, 2),
                "assessment": "EXCELLENT" if current_throughput >= 1000 else
                            "GOOD" if current_throughput >= 500 else
                            "ACCEPTABLE" if current_throughput >= 100 else
                            "POOR" if current_throughput >= 50 else "CRITICAL"
            },
            "processing": {
                "avg_time_ms": round(avg_processing_time, 2),
                "assessment": "EXCELLENT" if avg_processing_time <= 10 else
                            "GOOD" if avg_processing_time <= 50 else
                            "ACCEPTABLE" if avg_processing_time <= 200 else
                            "POOR" if avg_processing_time <= 500 else "CRITICAL"
            },
            "volume": {
                "messages_processed": performance_stats["messages_processed"],
                "messages_failed": performance_stats["messages_failed"],
                "relationships_created": performance_stats["relationships_created"]
            },
            "graph": {
                "total_nodes": neo4j_stats.get("total_nodes", 0),
                "total_relationships": neo4j_stats.get("total_relationships", 0),
                "relationship_density": round(
                    neo4j_stats.get("total_relationships", 0) / max(neo4j_stats.get("total_nodes", 1), 1), 3
                )
            },
            "scaling": {
                "recommendation": get_scaling_recommendation(current_throughput, avg_processing_time),
                "boundary_status": "WITHIN_LIMITS" if current_throughput >= 100 else "APPROACHING_LIMITS"
            }
        }), 200
        
    except Exception as e:
        logger.error("Error getting real-time metrics", error=str(e))
        return jsonify({"error": str(e)}), 500


@app.route('/metrics/boundary-analysis', methods=['GET'])
def boundary_analysis():
    """Boundary analysis and scaling recommendations"""
    try:
        uptime = time.time() - performance_stats["start_time"]
        current_throughput = performance_stats["messages_processed"] / max(uptime, 1)
        avg_processing_time = (performance_stats["total_processing_time"] / 
                             max(performance_stats["messages_processed"], 1)) * 1000
        
        # Define scaling boundaries
        boundaries = {
            "current_architecture_limit": 10000,      # messages/batch
            "optimization_required_above": 5000,      # messages/batch
            "dataflow_recommended_above": 50000,      # messages/batch
            "current_throughput_sustainable": current_throughput >= 100
        }
        
        # Performance projections
        projections = {
            "10k_messages_eta": round(10000 / max(current_throughput, 1), 1),
            "50k_messages_eta": round(50000 / max(current_throughput, 1), 1),
            "100k_messages_eta": round(100000 / max(current_throughput, 1), 1)
        }
        
        # Bottleneck analysis
        bottlenecks = []
        if avg_processing_time > 200:
            bottlenecks.append("NEO4J_WRITE_PERFORMANCE")
        if current_throughput < 100:
            bottlenecks.append("PUBSUB_PULL_RATE")
        if current_throughput < 50:
            bottlenecks.append("OVERALL_SYSTEM_CAPACITY")
        
        return jsonify({
            "current_performance": {
                "throughput_msg_per_sec": round(current_throughput, 2),
                "avg_processing_time_ms": round(avg_processing_time, 2),
                "messages_processed": performance_stats["messages_processed"]
            },
            "scaling_boundaries": boundaries,
            "performance_projections": projections,
            "bottleneck_analysis": {
                "identified_bottlenecks": bottlenecks,
                "primary_bottleneck": bottlenecks[0] if bottlenecks else "NO_BOTTLENECK_DETECTED"
            },
            "architecture_recommendations": {
                "current_suitability": "GOOD" if current_throughput >= 100 else "NEEDS_OPTIMIZATION",
                "scale_to_dataflow_when": "Processing >50K messages/batch OR throughput <50 msg/sec",
                "optimization_options": [
                    "Increase Cloud Run concurrency",
                    "Optimize Neo4j batch writes", 
                    "Use multiple processor instances",
                    "Implement message batching"
                ]
            }
        }), 200
        
    except Exception as e:
        logger.error("Error in boundary analysis", error=str(e))
        return jsonify({"error": str(e)}), 500


def graceful_shutdown(signum, frame):
    """Handle graceful shutdown"""
    logger.info("Received shutdown signal", signal=signum)
    neo4j_service.close()
    executor.shutdown(wait=True)
    sys.exit(0)


if __name__ == '__main__':
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, graceful_shutdown)
    signal.signal(signal.SIGINT, graceful_shutdown)
    
    port = int(os.getenv('PORT', 8080))
    
    # Set up pull subscriber for local development
    pull_config = setup_pull_subscriber()
    if pull_config:
        subscriber, subscription_path, callback = pull_config
        flow_control = pubsub_v1.types.FlowControl(max_messages=500)  # Increased for throughput
        streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback, flow_control=flow_control)
        logger.info("Pull subscriber started", subscription_path=subscription_path)
        print(f"🏥 Listening for healthcare messages on {subscription_path}...")
        print("📊 High-performance healthcare data processor ready!")
    
    logger.info("Starting Healthcare Processor", port=port)
    app.run(host='0.0.0.0', port=port, debug=False)
