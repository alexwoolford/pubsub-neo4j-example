#!/usr/bin/env python3
"""
Healthcare Demo Automation
Complete infrastructure provisioning, demo execution, and cleanup
"""

import subprocess
import time
import argparse

class HealthcareDemo:
    """Complete demo automation with infrastructure management"""
    
    def __init__(self, project_id: str, region: str = "us-central1"):
        self.project_id = project_id
        self.region = region
        self.service_name = "healthcare-processor"
        self.topic_name = "neo4j-topic"
        self.subscription_name = "neo4j-subscription"
        
        # Track what we've created for cleanup
        self.created_resources = {
            "cloud_run_service": False,
            "pubsub_topic": False,
            "pubsub_subscription": False,
            "container_image": False
        }
        
        # Process tracking
        self.local_processes = []
    
    def run_complete_demo(self, demo_type="quick"):
        """Run complete automated demo"""
        print("🏥 HEALTHCARE STREAMING DEMO")
        print("="*60)
        print(f"Project: {self.project_id}")
        print(f"Region: {self.region}")
        print(f"Demo Type: {demo_type}")
        print()
        
        try:
            # 1. Setup infrastructure
            print("🚀 Phase 1: Infrastructure Setup")
            self._setup_infrastructure()
            
            # 2. Deploy application
            print("\n🏗️ Phase 2: Application Deployment")
            if demo_type == "cloud":
                self._deploy_to_cloud_run()
                app_url = self._get_cloud_run_url()
            else:
                print("Running local demo (no Cloud Run deployment)")
                app_url = "http://localhost:8080"
            
            # 3. Run performance demo
            print("\n📊 Phase 3: Performance Demonstration")
            self._run_performance_demo(app_url, demo_type)
            
            # 4. Show results
            print("\n📈 Phase 4: Results Analysis")
            self._show_demo_results(app_url)
            
            print("\n✅ Demo completed successfully!")
            
        except KeyboardInterrupt:
            print("\n⚠️ Demo interrupted by user")
        except Exception as e:
            print(f"\n❌ Demo failed: {e}")
        finally:
            # Always offer cleanup
            self._offer_cleanup()
    
    def _setup_infrastructure(self):
        """Setup required Google Cloud infrastructure"""
        print("  📡 Enabling required APIs...")
        self._enable_apis()
        
        print("  📨 Setting up Pub/Sub...")
        self._setup_pubsub()
        
        print("  🔒 Configuring IAM permissions...")
        self._setup_iam()
        
        print("  ✅ Infrastructure setup complete")
    
    def _enable_apis(self):
        """Enable required Google Cloud APIs"""
        apis = [
            "pubsub.googleapis.com",
            "run.googleapis.com", 
            "cloudbuild.googleapis.com",
            "container.googleapis.com"
        ]
        
        for api in apis:
            try:
                subprocess.run([
                    "gcloud", "services", "enable", api,
                    "--project", self.project_id
                ], check=True, capture_output=True)
                print(f"    ✅ {api}")
            except subprocess.CalledProcessError:
                print(f"    ⚠️ {api} (already enabled or failed)")
    
    def _setup_pubsub(self):
        """Setup Pub/Sub topic and subscriptions"""
        # Create topic
        try:
            subprocess.run([
                "gcloud", "pubsub", "topics", "create", self.topic_name,
                "--project", self.project_id
            ], check=True, capture_output=True)
            self.created_resources["pubsub_topic"] = True
            print(f"    ✅ Created topic: {self.topic_name}")
        except subprocess.CalledProcessError:
            print(f"    ⚠️ Topic {self.topic_name} already exists")
        
        # Create pull subscription (for local development)
        try:
            subprocess.run([
                "gcloud", "pubsub", "subscriptions", "create", self.subscription_name,
                "--topic", self.topic_name,
                "--project", self.project_id
            ], check=True, capture_output=True)
            self.created_resources["pubsub_subscription"] = True
            print(f"    ✅ Created subscription: {self.subscription_name}")
        except subprocess.CalledProcessError:
            print(f"    ⚠️ Subscription {self.subscription_name} already exists")
    
    def _setup_iam(self):
        """Setup IAM permissions"""
        # This would set up proper IAM roles
        # For demo purposes, assuming user has sufficient permissions
        print("    ✅ IAM permissions (using current user credentials)")
    
    def _deploy_to_cloud_run(self):
        """Deploy to Cloud Run"""
        print("  🐳 Building container image...")
        
        # Build and push container
        image_name = f"gcr.io/{self.project_id}/{self.service_name}"
        
        try:
            # Build with Cloud Build
            subprocess.run([
                "gcloud", "builds", "submit", "--tag", image_name,
                "--project", self.project_id
            ], check=True)
            self.created_resources["container_image"] = True
            print(f"    ✅ Container built: {image_name}")
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to build container: {e}")
        
        print("  🚀 Deploying to Cloud Run...")
        
        try:
            # Deploy to Cloud Run
            subprocess.run([
                "gcloud", "run", "deploy", self.service_name,
                "--image", image_name,
                "--platform", "managed",
                "--region", self.region,
                "--allow-unauthenticated",
                "--memory", "2Gi",
                "--cpu", "2",
                "--concurrency", "100",
                "--max-instances", "10",
                "--set-env-vars", f"GOOGLE_CLOUD_PROJECT={self.project_id}",
                "--set-env-vars", f"PUBSUB_SUBSCRIPTION={self.subscription_name}",
                "--project", self.project_id
            ], check=True)
            self.created_resources["cloud_run_service"] = True
            print(f"    ✅ Deployed to Cloud Run: {self.service_name}")
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to deploy to Cloud Run: {e}")
        
        # Create push subscription for Cloud Run
        service_url = self._get_cloud_run_url()
        webhook_url = f"{service_url}/webhook"
        
        try:
            subprocess.run([
                "gcloud", "pubsub", "subscriptions", "create", f"{self.subscription_name}-push",
                "--topic", self.topic_name,
                "--push-endpoint", webhook_url,
                "--project", self.project_id
            ], check=True, capture_output=True)
            print(f"    ✅ Created push subscription to: {webhook_url}")
        except subprocess.CalledProcessError:
            print("    ⚠️ Push subscription already exists")
    
    def _get_cloud_run_url(self):
        """Get Cloud Run service URL"""
        try:
            result = subprocess.run([
                "gcloud", "run", "services", "describe", self.service_name,
                "--region", self.region,
                "--format", "value(status.url)",
                "--project", self.project_id
            ], capture_output=True, text=True, check=True)
            return result.stdout.strip()
        except subprocess.CalledProcessError:
            return None
    
    def _run_performance_demo(self, app_url: str, demo_type: str):
        """Run performance demonstration"""
        if demo_type == "local" or app_url == "http://localhost:8080":
            self._run_local_demo()
        else:
            self._run_cloud_demo(app_url)
    
    def _run_local_demo(self):
        """Run local demonstration"""
        print("  🏠 Starting local healthcare processor...")
        
        # Start local processor
        process = subprocess.Popen(
            ["python", "healthcare_app.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        self.local_processes.append(process)
        
        # Wait for it to be ready
        time.sleep(5)
        
        # Test connection
        try:
            import requests
            response = requests.get("http://localhost:8080/health", timeout=5)
            if response.status_code == 200:
                print("    ✅ Local processor ready")
            else:
                raise Exception("Health check failed")
        except Exception as e:
            raise Exception(f"Failed to start local processor: {e}")
        
        # Run publisher
        print("  📤 Publishing healthcare data...")
        subprocess.run([
            "python", "healthcare_publisher.py",
            "--project-id", self.project_id,
            "--mode", "small"
        ], check=True)
        
        print("    ✅ Messages published")
        
        # Wait for processing
        print("  ⏳ Waiting for message processing...")
        time.sleep(10)
    
    def _run_cloud_demo(self, app_url: str):
        """Run cloud demonstration"""
        print(f"  ☁️ Using Cloud Run service: {app_url}")
        
        # Wait for service to be ready
        print("  ⏳ Waiting for Cloud Run service to be ready...")
        time.sleep(10)
        
        # Test connection
        try:
            import requests
            response = requests.get(f"{app_url}/health", timeout=10)
            if response.status_code == 200:
                print("    ✅ Cloud Run service ready")
            else:
                raise Exception("Health check failed")
        except Exception as e:
            raise Exception(f"Cloud Run service not ready: {e}")
        
        # Run publisher
        print("  📤 Publishing healthcare data...")
        subprocess.run([
            "python", "healthcare_publisher.py",
            "--project-id", self.project_id,
            "--mode", "medium"
        ], check=True)
        
        print("    ✅ Messages published")
        
        # Wait for processing
        print("  ⏳ Waiting for message processing...")
        time.sleep(20)
    
    def _show_demo_results(self, app_url: str):
        """Show demo results"""
        try:
            import requests
            
            # Get final statistics
            response = requests.get(f"{app_url}/stats", timeout=10)
            if response.status_code == 200:
                stats = response.json()
                
                perf = stats.get("performance_statistics", {})
                db = stats.get("database_statistics", {})
                
                print("  📊 DEMO RESULTS:")
                print(f"    Messages Processed: {perf.get('messages_processed', 0):,}")
                print(f"    Throughput: {perf.get('throughput_msg_per_sec', 0):.1f} msg/sec")
                print(f"    Success Rate: {perf.get('success_rate', 0):.1f}%")
                print(f"    Graph Nodes: {db.get('total_nodes', 0):,}")
                print(f"    Graph Relationships: {db.get('total_relationships', 0):,}")
                
                # Get boundary analysis
                boundary_response = requests.get(f"{app_url}/metrics/boundary-analysis", timeout=10)
                if boundary_response.status_code == 200:
                    boundary = boundary_response.json()
                    print(f"    Scaling Recommendation: {boundary['architecture_recommendations']['current_suitability']}")
            
        except Exception as e:
            print(f"    ⚠️ Could not retrieve results: {e}")
    
    def _offer_cleanup(self):
        """Offer to clean up resources"""
        print("\n🧹 CLEANUP OPTIONS:")
        print("  The following resources were created/used:")
        
        if self.created_resources["cloud_run_service"]:
            print(f"    • Cloud Run service: {self.service_name}")
        if self.created_resources["pubsub_topic"]:
            print(f"    • Pub/Sub topic: {self.topic_name}")
        if self.created_resources["pubsub_subscription"]:
            print(f"    • Pub/Sub subscription: {self.subscription_name}")
        if self.created_resources["container_image"]:
            print(f"    • Container image: gcr.io/{self.project_id}/{self.service_name}")
        
        print("    • Neo4j Aura: (external service, not managed)")
        
        cleanup = input("\nDelete created resources? (y/N): ").lower().strip()
        
        if cleanup == 'y':
            self._cleanup_resources()
        else:
            print("Resources preserved. You can manually delete them later.")
            self._show_cleanup_commands()
    
    def _cleanup_resources(self):
        """Clean up created resources"""
        print("\n🗑️ Cleaning up resources...")
        
        # Stop local processes
        for process in self.local_processes:
            process.terminate()
            process.wait()
        
        # Delete Cloud Run service
        if self.created_resources["cloud_run_service"]:
            try:
                subprocess.run([
                    "gcloud", "run", "services", "delete", self.service_name,
                    "--region", self.region,
                    "--quiet",
                    "--project", self.project_id
                ], check=True, capture_output=True)
                print(f"  ✅ Deleted Cloud Run service: {self.service_name}")
            except Exception:
                print("  ⚠️ Failed to delete Cloud Run service")
        
        # Delete Pub/Sub resources
        if self.created_resources["pubsub_subscription"]:
            try:
                subprocess.run([
                    "gcloud", "pubsub", "subscriptions", "delete", self.subscription_name,
                    "--quiet",
                    "--project", self.project_id
                ], check=True, capture_output=True)
                print(f"  ✅ Deleted subscription: {self.subscription_name}")
            except Exception:
                print("  ⚠️ Failed to delete subscription")
        
        if self.created_resources["pubsub_topic"]:
            try:
                subprocess.run([
                    "gcloud", "pubsub", "topics", "delete", self.topic_name,
                    "--quiet",
                    "--project", self.project_id
                ], check=True, capture_output=True)
                print(f"  ✅ Deleted topic: {self.topic_name}")
            except Exception:
                print("  ⚠️ Failed to delete topic")
        
        print("  💰 Estimated monthly savings: ~$15-25")
        print("  ✅ Cleanup complete!")
    
    def _show_cleanup_commands(self):
        """Show manual cleanup commands"""
        print("\n📋 MANUAL CLEANUP COMMANDS:")
        print("# Delete Cloud Run service:")
        print(f"gcloud run services delete {self.service_name} --region {self.region}")
        print("")
        print("# Delete Pub/Sub resources:")
        print(f"gcloud pubsub subscriptions delete {self.subscription_name}")
        print(f"gcloud pubsub topics delete {self.topic_name}")
        print("")
        print("# Delete container images:")
        print(f"gcloud container images delete gcr.io/{self.project_id}/{self.service_name}")


def main():
    parser = argparse.ArgumentParser(description='Healthcare Demo Automation')
    parser.add_argument('--project-id', required=True, help='Google Cloud Project ID')
    parser.add_argument('--region', default='us-central1', help='Google Cloud region')
    parser.add_argument('--demo-type', choices=['local', 'cloud', 'quick'], 
                       default='local', help='Demo type')
    parser.add_argument('--cleanup-only', action='store_true', 
                       help='Only run cleanup of existing resources')
    
    args = parser.parse_args()
    
    demo = HealthcareDemo(args.project_id, args.region)
    
    if args.cleanup_only:
        demo._offer_cleanup()
    else:
        demo.run_complete_demo(args.demo_type)


if __name__ == "__main__":
    main()
