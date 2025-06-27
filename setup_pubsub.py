#!/usr/bin/env python3
"""
Script to set up Google Cloud Pub/Sub infrastructure for the Neo4j example.
This script creates the topic and subscription needed for the application.
"""

import os
import argparse
from google.cloud import pubsub_v1
from google.api_core import exceptions
import structlog

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


def create_topic(project_id, topic_name):
    """Create a Pub/Sub topic"""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    
    try:
        topic = publisher.create_topic(request={"name": topic_path})
        logger.info("Created topic", topic=topic.name)
        return topic
    except exceptions.AlreadyExists:
        logger.info("Topic already exists", topic=topic_path)
        return publisher.get_topic(request={"topic": topic_path})


def create_push_subscription(project_id, topic_name, subscription_name, push_endpoint):
    """Create a push subscription for Cloud Run"""
    subscriber = pubsub_v1.SubscriberClient()
    publisher = pubsub_v1.PublisherClient()
    
    topic_path = publisher.topic_path(project_id, topic_name)
    subscription_path = subscriber.subscription_path(project_id, subscription_name)
    
    push_config = pubsub_v1.PushConfig(push_endpoint=push_endpoint)
    
    try:
        subscription = subscriber.create_subscription(
            request={
                "name": subscription_path,
                "topic": topic_path,
                "push_config": push_config,
                "ack_deadline_seconds": 60,
                "message_retention_duration": {"seconds": 7 * 24 * 60 * 60},  # 7 days
            }
        )
        logger.info("Created push subscription", 
                   subscription=subscription.name,
                   push_endpoint=push_endpoint)
        return subscription
    except exceptions.AlreadyExists:
        logger.info("Subscription already exists", subscription=subscription_path)
        return subscriber.get_subscription(request={"subscription": subscription_path})


def create_pull_subscription(project_id, topic_name, subscription_name):
    """Create a pull subscription for local development"""
    subscriber = pubsub_v1.SubscriberClient()
    publisher = pubsub_v1.PublisherClient()
    
    topic_path = publisher.topic_path(project_id, topic_name)
    subscription_path = subscriber.subscription_path(project_id, subscription_name)
    
    try:
        subscription = subscriber.create_subscription(
            request={
                "name": subscription_path,
                "topic": topic_path,
                "ack_deadline_seconds": 60,
                "message_retention_duration": {"seconds": 7 * 24 * 60 * 60},  # 7 days
            }
        )
        logger.info("Created pull subscription", subscription=subscription.name)
        return subscription
    except exceptions.AlreadyExists:
        logger.info("Subscription already exists", subscription=subscription_path)
        return subscriber.get_subscription(request={"subscription": subscription_path})


def main():
    parser = argparse.ArgumentParser(description='Set up Pub/Sub infrastructure')
    parser.add_argument('--project-id', required=True, help='Google Cloud Project ID')
    parser.add_argument('--topic', default='neo4j-topic', help='Pub/Sub topic name')
    parser.add_argument('--subscription', default='neo4j-subscription', help='Subscription name')
    parser.add_argument('--push-endpoint', help='Cloud Run webhook endpoint URL')
    parser.add_argument('--pull-only', action='store_true', help='Create pull subscription only (for local dev)')
    
    args = parser.parse_args()
    
    logger.info("Setting up Pub/Sub infrastructure",
               project_id=args.project_id,
               topic=args.topic,
               subscription=args.subscription)
    
    # Create topic
    create_topic(args.project_id, args.topic)
    
    # Create subscription
    if args.pull_only or not args.push_endpoint:
        create_pull_subscription(args.project_id, args.topic, args.subscription)
    else:
        create_push_subscription(args.project_id, args.topic, args.subscription, args.push_endpoint)
    
    logger.info("Pub/Sub setup completed successfully")
    
    # Print helpful information
    print("\n" + "="*60)
    print("PUB/SUB SETUP COMPLETED")
    print("="*60)
    print(f"Topic: {args.topic}")
    print(f"Subscription: {args.subscription}")
    if args.push_endpoint:
        print(f"Push Endpoint: {args.push_endpoint}")
    print("\nTo publish test messages, run:")
    print(f"python publish_messages.py --project-id {args.project_id} --topic {args.topic}")
    print("="*60)


if __name__ == "__main__":
    main() 