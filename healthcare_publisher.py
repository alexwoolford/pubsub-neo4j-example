#!/usr/bin/env python3
"""
High-Performance Healthcare Data Publisher
Publishes thousands of connected healthcare messages to Pub/Sub with throughput measurement
"""

import json
import time
import argparse
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import pubsub_v1
import structlog
from healthcare_data_generator import HealthcareDataGenerator

# Configure logging
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


class HealthcarePublisherMetrics:
    """Track publishing performance metrics"""
    
    def __init__(self):
        self.lock = threading.Lock()
        self.start_time = None
        self.end_time = None
        self.messages_sent = 0
        self.messages_failed = 0
        self.total_bytes_sent = 0
        self.batch_times = []
        
    def start_publishing(self):
        with self.lock:
            self.start_time = time.time()
            
    def record_success(self, message_size):
        with self.lock:
            self.messages_sent += 1
            self.total_bytes_sent += message_size
            
    def record_failure(self):
        with self.lock:
            self.messages_failed += 1
            
    def record_batch_time(self, batch_time):
        with self.lock:
            self.batch_times.append(batch_time)
            
    def finish_publishing(self):
        with self.lock:
            self.end_time = time.time()
            
    def get_stats(self):
        with self.lock:
            if not self.start_time:
                return {}
                
            duration = (self.end_time or time.time()) - self.start_time
            total_messages = self.messages_sent + self.messages_failed
            
            return {
                "duration_seconds": round(duration, 2),
                "total_messages": total_messages,
                "messages_sent": self.messages_sent,
                "messages_failed": self.messages_failed,
                "success_rate_percent": round((self.messages_sent / max(total_messages, 1)) * 100, 2),
                "throughput_msg_per_sec": round(self.messages_sent / max(duration, 0.01), 2),
                "total_bytes_sent": self.total_bytes_sent,
                "avg_message_size": round(self.total_bytes_sent / max(self.messages_sent, 1), 2),
                "throughput_mb_per_sec": round((self.total_bytes_sent / (1024*1024)) / max(duration, 0.01), 4),
                "avg_batch_time_ms": round(sum(self.batch_times) / max(len(self.batch_times), 1) * 1000, 2) if self.batch_times else 0
            }


def publish_message_batch(publisher, topic_path, messages, batch_id, metrics):
    """Publish a batch of messages with error handling"""
    batch_start = time.time()
    batch_success = 0
    batch_failed = 0
    
    futures = []
    
    try:
        # Submit all messages in the batch
        for message in messages:
            message_json = json.dumps(message)
            data = message_json.encode('utf-8')
            
            # Add message attributes for routing/filtering
            attributes = {
                'message_type': message.get('type', 'unknown'),
                'entity_id': message.get('id', ''),
                'batch_id': str(batch_id)
            }
            
            future = publisher.publish(topic_path, data, **attributes)
            futures.append((future, len(data)))
        
        # Wait for all messages in batch to complete
        for future, message_size in futures:
            try:
                message_id = future.result(timeout=10)  # 10 second timeout per message
                metrics.record_success(message_size)
                batch_success += 1
                
            except Exception as e:
                metrics.record_failure()
                batch_failed += 1
                logger.error("Failed to publish message in batch", 
                           batch_id=batch_id, error=str(e))
    
    except Exception as e:
        logger.error("Batch publishing failed", batch_id=batch_id, error=str(e))
        metrics.record_failure()
        return 0, len(messages)
    
    batch_time = time.time() - batch_start
    metrics.record_batch_time(batch_time)
    
    logger.info("Batch completed", 
               batch_id=batch_id, 
               success=batch_success, 
               failed=batch_failed,
               batch_time_ms=round(batch_time * 1000, 2))
    
    return batch_success, batch_failed


def publish_healthcare_data_high_performance(project_id, topic_name, 
                                           doctors=100, patients=500, 
                                           diagnoses=1000, medications=1500, 
                                           procedures=800,
                                           batch_size=50, max_workers=10):
    """Publish healthcare data with high performance and throughput measurement"""
    
    print(f"\n🏥 HEALTHCARE DATA PUBLISHER")
    print(f"{'='*60}")
    print(f"Target: {project_id}/{topic_name}")
    print(f"Dataset: {doctors} doctors, {patients} patients")
    print(f"Clinical data: {diagnoses} diagnoses, {medications} medications, {procedures} procedures")
    print(f"Performance: {batch_size} messages/batch, {max_workers} concurrent workers")
    print(f"{'='*60}")
    
    # Initialize components
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    generator = HealthcareDataGenerator()
    metrics = HealthcarePublisherMetrics()
    
    # Generate the complete healthcare dataset
    print(f"\n📊 Generating healthcare dataset...")
    dataset_start = time.time()
    
    healthcare_data = generator.generate_complete_dataset(
        doctors=doctors,
        patients=patients,
        diagnoses=diagnoses,
        medications=medications,
        procedures=procedures
    )
    
    dataset_time = time.time() - dataset_start
    print(f"✅ Dataset generated in {dataset_time:.2f} seconds")
    print(f"📈 Total records: {len(healthcare_data)}")
    
    # Split into batches
    batches = [healthcare_data[i:i + batch_size] 
              for i in range(0, len(healthcare_data), batch_size)]
    
    print(f"\n🚀 Publishing {len(healthcare_data)} messages in {len(batches)} batches...")
    print(f"⏱️  Starting high-performance publishing...")
    
    metrics.start_publishing()
    
    # Publish batches concurrently
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {
            executor.submit(publish_message_batch, publisher, topic_path, batch, i, metrics): i
            for i, batch in enumerate(batches)
        }
        
        total_success = 0
        total_failed = 0
        completed_batches = 0
        
        for future in as_completed(future_to_batch):
            batch_id = future_to_batch[future]
            try:
                success, failed = future.result()
                total_success += success
                total_failed += failed
                completed_batches += 1
                
                # Progress update every 10 batches
                if completed_batches % 10 == 0 or completed_batches == len(batches):
                    progress = (completed_batches / len(batches)) * 100
                    current_stats = metrics.get_stats()
                    print(f"📊 Progress: {progress:.1f}% | "
                          f"Sent: {total_success} | "
                          f"Failed: {total_failed} | "
                          f"Throughput: {current_stats.get('throughput_msg_per_sec', 0):.1f} msg/sec")
                
            except Exception as e:
                logger.error("Batch future failed", batch_id=batch_id, error=str(e))
                total_failed += len(batches[batch_id])
    
    metrics.finish_publishing()
    
    # Final statistics
    final_stats = metrics.get_stats()
    
    print(f"\n🎉 HIGH-PERFORMANCE PUBLISHING COMPLETED!")
    print(f"{'='*60}")
    print(f"📊 FINAL STATISTICS:")
    print(f"  Duration: {final_stats['duration_seconds']} seconds")
    print(f"  Total Messages: {final_stats['total_messages']}")
    print(f"  Successfully Sent: {final_stats['messages_sent']}")
    print(f"  Failed: {final_stats['messages_failed']}")
    print(f"  Success Rate: {final_stats['success_rate_percent']}%")
    print(f"  Throughput: {final_stats['throughput_msg_per_sec']} messages/second")
    print(f"  Data Volume: {final_stats['total_bytes_sent']:,} bytes ({final_stats['throughput_mb_per_sec']} MB/sec)")
    print(f"  Avg Message Size: {final_stats['avg_message_size']} bytes")
    print(f"  Avg Batch Time: {final_stats['avg_batch_time_ms']} ms")
    print(f"{'='*60}")
    
    # Performance assessment
    throughput = final_stats['throughput_msg_per_sec']
    if throughput >= 1000:
        print(f"🚀 EXCELLENT: {throughput:.0f} msg/sec - Ready for production scale!")
    elif throughput >= 500:
        print(f"✅ GOOD: {throughput:.0f} msg/sec - Suitable for most use cases")
    elif throughput >= 100:
        print(f"⚠️  MODERATE: {throughput:.0f} msg/sec - Consider optimization for high volume")
    else:
        print(f"❌ LOW: {throughput:.0f} msg/sec - Performance optimization needed")
    
    return final_stats


def run_throughput_test(project_id, topic_name, test_sizes):
    """Run throughput tests with different dataset sizes"""
    print(f"\n🔬 THROUGHPUT BOUNDARY TESTING")
    print(f"{'='*60}")
    
    results = []
    
    for test_name, config in test_sizes.items():
        print(f"\n📊 Running {test_name}...")
        stats = publish_healthcare_data_high_performance(
            project_id=project_id,
            topic_name=topic_name,
            **config
        )
        
        results.append({
            "test_name": test_name,
            "config": config,
            "stats": stats
        })
        
        # Brief pause between tests
        time.sleep(2)
    
    # Summary report
    print(f"\n📋 THROUGHPUT TEST SUMMARY")
    print(f"{'='*60}")
    print(f"{'Test Name':<20} {'Messages':<10} {'Duration(s)':<12} {'Throughput':<15} {'Success Rate'}")
    print(f"{'-'*60}")
    
    for result in results:
        stats = result['stats']
        config = result['config']
        total_messages = sum(config.values())
        
        print(f"{result['test_name']:<20} "
              f"{stats['messages_sent']:<10} "
              f"{stats['duration_seconds']:<12} "
              f"{stats['throughput_msg_per_sec']:<15.1f} "
              f"{stats['success_rate_percent']:<10.1f}%")
    
    print(f"{'='*60}")
    return results


def main():
    parser = argparse.ArgumentParser(description='High-Performance Healthcare Data Publisher')
    parser.add_argument('--project-id', required=True, help='Google Cloud Project ID')
    parser.add_argument('--topic', default='neo4j-topic', help='Pub/Sub topic name')
    parser.add_argument('--mode', choices=['small', 'medium', 'large', 'massive', 'test-suite'], 
                       default='medium', help='Publishing mode')
    parser.add_argument('--batch-size', type=int, default=50, help='Messages per batch')
    parser.add_argument('--max-workers', type=int, default=10, help='Concurrent workers')
    
    # Custom dataset sizes
    parser.add_argument('--doctors', type=int, help='Number of doctors')
    parser.add_argument('--patients', type=int, help='Number of patients')
    parser.add_argument('--diagnoses', type=int, help='Number of diagnoses')
    parser.add_argument('--medications', type=int, help='Number of medications')
    parser.add_argument('--procedures', type=int, help='Number of procedures')
    
    args = parser.parse_args()
    
    if args.mode == 'test-suite':
        # Run comprehensive throughput tests
        test_configs = {
            "Small (1K)": {"doctors": 20, "patients": 100, "diagnoses": 200, "medications": 300, "procedures": 150},
            "Medium (3K)": {"doctors": 50, "patients": 300, "diagnoses": 600, "medications": 800, "procedures": 400},
            "Large (10K)": {"doctors": 100, "patients": 1000, "diagnoses": 2000, "medications": 3000, "procedures": 1500},
            "Massive (50K)": {"doctors": 500, "patients": 5000, "diagnoses": 10000, "medications": 15000, "procedures": 7500}
        }
        
        run_throughput_test(args.project_id, args.topic, test_configs)
        
    else:
        # Single test run
        if all([args.doctors, args.patients, args.diagnoses, args.medications, args.procedures]):
            # Custom configuration
            config = {
                "doctors": args.doctors,
                "patients": args.patients,
                "diagnoses": args.diagnoses,
                "medications": args.medications,
                "procedures": args.procedures,
                "batch_size": args.batch_size,
                "max_workers": args.max_workers
            }
        else:
            # Predefined configurations
            configs = {
                "small": {"doctors": 20, "patients": 100, "diagnoses": 200, "medications": 300, "procedures": 150},
                "medium": {"doctors": 50, "patients": 300, "diagnoses": 600, "medications": 800, "procedures": 400},
                "large": {"doctors": 100, "patients": 1000, "diagnoses": 2000, "medications": 3000, "procedures": 1500},
                "massive": {"doctors": 500, "patients": 5000, "diagnoses": 10000, "medications": 15000, "procedures": 7500}
            }
            
            config = configs[args.mode].copy()
            config.update({
                "batch_size": args.batch_size,
                "max_workers": args.max_workers
            })
        
        publish_healthcare_data_high_performance(
            project_id=args.project_id,
            topic_name=args.topic,
            **config
        )


if __name__ == "__main__":
    main() 