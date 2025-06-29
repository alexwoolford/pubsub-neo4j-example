#!/usr/bin/env python3
"""
Healthcare Data Generator
Generates realistic clinical data with meaningful relationships for the graph.
"""

import json
import random
from datetime import datetime, timedelta
from typing import List, Dict

class HealthcareDataGenerator:
    """Generate realistic healthcare data with relationships"""
    
    def __init__(self):
        self.hospitals = [
            {"id": "h001", "name": "General Hospital", "location": "San Francisco", "type": "General"},
            {"id": "h002", "name": "Stanford Medical Center", "location": "Palo Alto", "type": "Research"},
            {"id": "h003", "name": "UCSF Medical Center", "location": "San Francisco", "type": "Academic"},
            {"id": "h004", "name": "Kaiser Permanente", "location": "Oakland", "type": "HMO"},
            {"id": "h005", "name": "Sutter Health", "location": "Sacramento", "type": "Network"}
        ]
        
        self.doctors = []
        self.patients = []
        self.generated_patient_ids = set()
        self.generated_doctor_ids = set()
        
        # Medical specialties
        self.specialties = [
            "Cardiology", "Oncology", "Neurology", "Endocrinology", 
            "Gastroenterology", "Pulmonology", "Nephrology", "Psychiatry",
            "Emergency Medicine", "Internal Medicine", "Family Medicine"
        ]
        
        # Common diagnoses with ICD-10 codes
        self.diagnoses = [
            {"code": "E11.9", "name": "Type 2 diabetes mellitus without complications", "severity": "moderate"},
            {"code": "I10", "name": "Essential hypertension", "severity": "mild"},
            {"code": "Z51.11", "name": "Encounter for antineoplastic chemotherapy", "severity": "severe"},
            {"code": "F41.1", "name": "Generalized anxiety disorder", "severity": "mild"},
            {"code": "M79.18", "name": "Myalgia, other site", "severity": "mild"},
            {"code": "R06.02", "name": "Shortness of breath", "severity": "moderate"},
            {"code": "K21.9", "name": "Gastro-esophageal reflux disease without esophagitis", "severity": "mild"},
            {"code": "M25.511", "name": "Pain in right shoulder", "severity": "moderate"},
            {"code": "I25.10", "name": "Atherosclerotic heart disease", "severity": "severe"},
            {"code": "N18.6", "name": "End stage renal disease", "severity": "severe"}
        ]
        
        # Common medications
        self.medications = [
            {"name": "Metformin", "dosage": "500mg", "frequency": "twice daily", "indication": "diabetes"},
            {"name": "Lisinopril", "dosage": "10mg", "frequency": "once daily", "indication": "hypertension"},
            {"name": "Atorvastatin", "dosage": "20mg", "frequency": "once daily", "indication": "cholesterol"},
            {"name": "Omeprazole", "dosage": "20mg", "frequency": "once daily", "indication": "acid reflux"},
            {"name": "Sertraline", "dosage": "50mg", "frequency": "once daily", "indication": "depression"},
            {"name": "Albuterol", "dosage": "90mcg", "frequency": "as needed", "indication": "asthma"},
            {"name": "Warfarin", "dosage": "5mg", "frequency": "once daily", "indication": "anticoagulation"},
            {"name": "Insulin", "dosage": "10 units", "frequency": "before meals", "indication": "diabetes"}
        ]
        
        # Common procedures
        self.procedures = [
            {"code": "93005", "name": "Electrocardiogram", "duration": 30, "type": "diagnostic"},
            {"code": "80053", "name": "Comprehensive metabolic panel", "duration": 15, "type": "lab"},
            {"code": "71020", "name": "Chest X-ray", "duration": 20, "type": "imaging"},
            {"code": "36415", "name": "Blood draw", "duration": 10, "type": "lab"},
            {"code": "99213", "name": "Office visit - established patient", "duration": 45, "type": "visit"},
            {"code": "45378", "name": "Colonoscopy", "duration": 60, "type": "procedure"},
            {"code": "76700", "name": "Abdominal ultrasound", "duration": 45, "type": "imaging"},
            {"code": "73721", "name": "MRI brain", "duration": 90, "type": "imaging"}
        ]
    
    def generate_doctors(self, count: int = 50) -> List[Dict]:
        """Generate doctor records"""
        first_names = ["Dr. Sarah", "Dr. Michael", "Dr. Jennifer", "Dr. David", "Dr. Lisa", 
                      "Dr. James", "Dr. Maria", "Dr. Robert", "Dr. Emily", "Dr. John"]
        last_names = ["Johnson", "Smith", "Williams", "Brown", "Jones", "Garcia", 
                     "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez"]
        
        doctors = []
        for i in range(count):
            doctor_id = f"doc_{i+1:03d}"
            self.generated_doctor_ids.add(doctor_id)
            
            doctor = {
                "type": "doctor",
                "id": doctor_id,
                "name": f"{random.choice(first_names)} {random.choice(last_names)}",
                "specialty": random.choice(self.specialties),
                "hospital_id": random.choice(self.hospitals)["id"],
                "license_number": f"MD{random.randint(100000, 999999)}",
                "years_experience": random.randint(2, 30),
                "timestamp": datetime.utcnow().isoformat()
            }
            doctors.append(doctor)
            self.doctors.append(doctor)
        
        return doctors
    
    def generate_patients(self, count: int = 200) -> List[Dict]:
        """Generate patient records"""
        first_names = ["John", "Jane", "Michael", "Sarah", "David", "Lisa", "James", "Maria", 
                      "Robert", "Jennifer", "William", "Patricia", "Richard", "Linda", "Thomas"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", 
                     "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson"]
        
        patients = []
        for i in range(count):
            patient_id = f"pat_{i+1:04d}"
            self.generated_patient_ids.add(patient_id)
            
            # Generate realistic age distribution
            age = random.choices(
                range(18, 95), 
                weights=[1 if x < 65 else 3 for x in range(18, 95)]  # Higher weight for older patients
            )[0]
            
            birth_date = datetime.now() - timedelta(days=age*365)
            
            patient = {
                "type": "patient",
                "id": patient_id,
                "name": f"{random.choice(first_names)} {random.choice(last_names)}",
                "mrn": f"MRN{random.randint(1000000, 9999999)}",  # Medical Record Number
                "date_of_birth": birth_date.strftime("%Y-%m-%d"),
                "age": age,
                "gender": random.choice(["Male", "Female", "Other"]),
                "phone": f"+1-{random.randint(100,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
                "email": f"patient{i+1}@email.com",
                "primary_care_doctor": random.choice(list(self.generated_doctor_ids)) if self.generated_doctor_ids else None,
                "timestamp": datetime.utcnow().isoformat()
            }
            patients.append(patient)
            self.patients.append(patient)
        
        return patients
    
    def generate_diagnoses(self, count: int = 500) -> List[Dict]:
        """Generate diagnosis records with patient relationships"""
        diagnoses = []
        for i in range(count):
            if not self.generated_patient_ids or not self.generated_doctor_ids:
                continue
                
            diagnosis_data = random.choice(self.diagnoses)
            diagnosed_date = datetime.now() - timedelta(days=random.randint(1, 365))
            
            diagnosis = {
                "type": "diagnosis",
                "id": f"diag_{i+1:04d}",
                "patient_id": random.choice(list(self.generated_patient_ids)),
                "doctor_id": random.choice(list(self.generated_doctor_ids)),
                "icd10_code": diagnosis_data["code"],
                "description": diagnosis_data["name"],
                "severity": diagnosis_data["severity"],
                "diagnosed_date": diagnosed_date.strftime("%Y-%m-%d"),
                "status": random.choice(["active", "resolved", "chronic", "in_remission"]),
                "timestamp": datetime.utcnow().isoformat()
            }
            diagnoses.append(diagnosis)
        
        return diagnoses
    
    def generate_medications(self, count: int = 800) -> List[Dict]:
        """Generate medication records"""
        medications = []
        for i in range(count):
            if not self.generated_patient_ids or not self.generated_doctor_ids:
                continue
                
            med_data = random.choice(self.medications)
            prescribed_date = datetime.now() - timedelta(days=random.randint(1, 180))
            
            medication = {
                "type": "medication",
                "id": f"med_{i+1:04d}",
                "patient_id": random.choice(list(self.generated_patient_ids)),
                "prescribing_doctor_id": random.choice(list(self.generated_doctor_ids)),
                "medication_name": med_data["name"],
                "dosage": med_data["dosage"],
                "frequency": med_data["frequency"],
                "indication": med_data["indication"],
                "prescribed_date": prescribed_date.strftime("%Y-%m-%d"),
                "quantity": random.randint(30, 90),
                "refills": random.randint(0, 5),
                "status": random.choice(["active", "discontinued", "completed"]),
                "timestamp": datetime.utcnow().isoformat()
            }
            medications.append(medication)
        
        return medications
    
    def generate_procedures(self, count: int = 400) -> List[Dict]:
        """Generate procedure records"""
        procedures = []
        for i in range(count):
            if not self.generated_patient_ids or not self.generated_doctor_ids:
                continue
                
            proc_data = random.choice(self.procedures)
            procedure_date = datetime.now() - timedelta(days=random.randint(1, 90))
            
            procedure = {
                "type": "procedure",
                "id": f"proc_{i+1:04d}",
                "patient_id": random.choice(list(self.generated_patient_ids)),
                "performing_doctor_id": random.choice(list(self.generated_doctor_ids)),
                "hospital_id": random.choice(self.hospitals)["id"],
                "cpt_code": proc_data["code"],
                "procedure_name": proc_data["name"],
                "procedure_type": proc_data["type"],
                "procedure_date": procedure_date.strftime("%Y-%m-%d"),
                "duration_minutes": proc_data["duration"],
                "status": random.choice(["completed", "scheduled", "in_progress", "cancelled"]),
                "cost": round(random.uniform(50, 5000), 2),
                "timestamp": datetime.utcnow().isoformat()
            }
            procedures.append(procedure)
        
        return procedures
    
    def generate_hospitals(self) -> List[Dict]:
        """Generate hospital records"""
        hospitals = []
        for hospital in self.hospitals:
            hospital_record = {
                "type": "hospital",
                "id": hospital["id"],
                "name": hospital["name"],
                "location": hospital["location"],
                "hospital_type": hospital["type"],
                "bed_count": random.randint(100, 800),
                "trauma_center": random.choice([True, False]),
                "teaching_hospital": random.choice([True, False]),
                "timestamp": datetime.utcnow().isoformat()
            }
            hospitals.append(hospital_record)
        
        return hospitals
    
    def generate_complete_dataset(self, 
                                 doctors=50, 
                                 patients=200, 
                                 diagnoses=500, 
                                 medications=800, 
                                 procedures=400) -> List[Dict]:
        """Generate a complete connected healthcare dataset"""
        print("Generating healthcare dataset:")
        print(f"  - {len(self.hospitals)} Hospitals")
        print(f"  - {doctors} Doctors")
        print(f"  - {patients} Patients") 
        print(f"  - {diagnoses} Diagnoses")
        print(f"  - {medications} Medications")
        print(f"  - {procedures} Procedures")
        
        all_records = []
        
        # Generate in order to establish relationships
        all_records.extend(self.generate_hospitals())
        all_records.extend(self.generate_doctors(doctors))
        all_records.extend(self.generate_patients(patients))
        all_records.extend(self.generate_diagnoses(diagnoses))
        all_records.extend(self.generate_medications(medications))
        all_records.extend(self.generate_procedures(procedures))
        
        print(f"\nTotal records generated: {len(all_records)}")
        return all_records


if __name__ == "__main__":
    generator = HealthcareDataGenerator()
    
    # Generate smaller dataset for testing
    dataset = generator.generate_complete_dataset(
        doctors=10,
        patients=50,
        diagnoses=100,
        medications=150,
        procedures=80
    )
    
    # Save to file for inspection
    with open('sample_healthcare_data.json', 'w') as f:
        json.dump(dataset[:10], f, indent=2)
    
    print("\nSample data saved to sample_healthcare_data.json")
    print(f"First record: {dataset[0]}")
