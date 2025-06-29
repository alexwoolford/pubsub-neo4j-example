import uuid
from datetime import datetime
from neo4j import GraphDatabase
import structlog

logger = structlog.get_logger()


class HealthcareNeo4jService:
    """Healthcare-specific Neo4j service with relationship modeling"""
    
    def __init__(self, uri, username, password, database=None):
        self.driver = GraphDatabase.driver(uri, auth=(username, password))
        self.database = database or "neo4j"
        logger.info("Initialized Healthcare Neo4j connection", uri=uri, username=username, database=self.database)
    
    def close(self):
        """Close the database connection"""
        if self.driver:
            self.driver.close()
            logger.info("Closed Neo4j connection")
    
    def test_connection(self):
        """Test the database connection"""
        with self.driver.session(database=self.database) as session:
            result = session.run("RETURN 1 as test")
            test_value = result.single()["test"]
            if test_value == 1:
                logger.info("Neo4j connection test successful", database=self.database)
                return True
            else:
                raise Exception("Neo4j connection test failed")
    
    def process_healthcare_message(self, message_data):
        """Process a healthcare message and create appropriate nodes and relationships"""
        with self.driver.session(database=self.database) as session:
            return session.execute_write(self._create_healthcare_entity, message_data)
    
    def _create_healthcare_entity(self, tx, message_data):
        """Create healthcare entities with relationships"""
        try:
            message_type = message_data.get('type', 'unknown').lower()
            entity_id = message_data.get('id', str(uuid.uuid4()))
            
            logger.info("Processing healthcare message", 
                       entity_type=message_type, 
                       entity_id=entity_id)
            
            if message_type == 'hospital':
                return self._create_hospital(tx, message_data)
            elif message_type == 'doctor':
                return self._create_doctor(tx, message_data)
            elif message_type == 'patient':
                return self._create_patient(tx, message_data)
            elif message_type == 'diagnosis':
                return self._create_diagnosis(tx, message_data)
            elif message_type == 'medication':
                return self._create_medication(tx, message_data)
            elif message_type == 'procedure':
                return self._create_procedure(tx, message_data)
            else:
                # Fallback for unknown types
                return self._create_generic_entity(tx, message_data)
                
        except Exception as e:
            logger.error("Error processing healthcare message", 
                        error=str(e), 
                        message_data=message_data)
            raise
    
    def _create_hospital(self, tx, data):
        """Create Hospital node"""
        query = """
        MERGE (h:Hospital {id: $id})
        SET h.name = $name,
            h.location = $location,
            h.hospital_type = $hospital_type,
            h.bed_count = $bed_count,
            h.trauma_center = $trauma_center,
            h.teaching_hospital = $teaching_hospital,
            h.updated_at = $timestamp
        RETURN h.id as entity_id
        """
        
        tx.run(query,
               id=data['id'],
                       name=data.get('name', ''),
                       location=data.get('location', ''),
                       hospital_type=data.get('hospital_type', ''),
                       bed_count=data.get('bed_count', 0),
                       trauma_center=data.get('trauma_center', False),
                       teaching_hospital=data.get('teaching_hospital', False),
                       timestamp=datetime.utcnow().isoformat())
        
        return {"entity_id": data['id'], "type": "Hospital", "relationships_created": 0}
    
    def _create_doctor(self, tx, data):
        """Create Doctor node and relationship to Hospital"""
        # Create doctor
        query = """
        MERGE (d:Doctor {id: $id})
        SET d.name = $name,
            d.specialty = $specialty,
            d.license_number = $license_number,
            d.years_experience = $years_experience,
            d.updated_at = $timestamp
        RETURN d.id as entity_id
        """
        
        tx.run(query,
               id=data['id'],
                       name=data.get('name', ''),
                       specialty=data.get('specialty', ''),
                       license_number=data.get('license_number', ''),
                       years_experience=data.get('years_experience', 0),
                       timestamp=datetime.utcnow().isoformat())
        
        relationships_created = 0
        
        # Create relationship to hospital
        hospital_id = data.get('hospital_id')
        if hospital_id:
            hospital_rel_query = """
            MATCH (d:Doctor {id: $doctor_id})
            MATCH (h:Hospital {id: $hospital_id})
            MERGE (d)-[:WORKS_AT]->(h)
            """
            tx.run(hospital_rel_query, doctor_id=data['id'], hospital_id=hospital_id)
            relationships_created += 1
        
        return {"entity_id": data['id'], "type": "Doctor", "relationships_created": relationships_created}
    
    def _create_patient(self, tx, data):
        """Create Patient node and relationship to primary care doctor"""
        query = """
        MERGE (p:Patient {id: $id})
        SET p.name = $name,
            p.mrn = $mrn,
            p.date_of_birth = $date_of_birth,
            p.age = $age,
            p.gender = $gender,
            p.phone = $phone,
            p.email = $email,
            p.updated_at = $timestamp
        RETURN p.id as entity_id
        """
        
        tx.run(query,
               id=data['id'],
                       name=data.get('name', ''),
                       mrn=data.get('mrn', ''),
                       date_of_birth=data.get('date_of_birth', ''),
                       age=data.get('age', 0),
                       gender=data.get('gender', ''),
                       phone=data.get('phone', ''),
                       email=data.get('email', ''),
                       timestamp=datetime.utcnow().isoformat())
        
        relationships_created = 0
        
        # Create relationship to primary care doctor
        primary_care_doctor = data.get('primary_care_doctor')
        if primary_care_doctor:
            doctor_rel_query = """
            MATCH (p:Patient {id: $patient_id})
            MATCH (d:Doctor {id: $doctor_id})
            MERGE (p)-[:HAS_PRIMARY_CARE_DOCTOR]->(d)
            """
            tx.run(doctor_rel_query, patient_id=data['id'], doctor_id=primary_care_doctor)
            relationships_created += 1
        
        return {"entity_id": data['id'], "type": "Patient", "relationships_created": relationships_created}
    
    def _create_diagnosis(self, tx, data):
        """Create Diagnosis node and relationships to Patient and Doctor"""
        query = """
        CREATE (diag:Diagnosis {
            id: $id,
            icd10_code: $icd10_code,
            description: $description,
            severity: $severity,
            diagnosed_date: $diagnosed_date,
            status: $status,
            created_at: $timestamp
        })
        RETURN diag.id as entity_id
        """
        
        tx.run(query,
               id=data['id'],
                       icd10_code=data.get('icd10_code', ''),
                       description=data.get('description', ''),
                       severity=data.get('severity', ''),
                       diagnosed_date=data.get('diagnosed_date', ''),
                       status=data.get('status', ''),
                       timestamp=datetime.utcnow().isoformat())
        
        relationships_created = 0
        
        # Create relationship to patient
        patient_id = data.get('patient_id')
        if patient_id:
            patient_rel_query = """
            MATCH (p:Patient {id: $patient_id})
            MATCH (diag:Diagnosis {id: $diagnosis_id})
            MERGE (p)-[:HAS_DIAGNOSIS]->(diag)
            """
            tx.run(patient_rel_query, patient_id=patient_id, diagnosis_id=data['id'])
            relationships_created += 1
        
        # Create relationship to diagnosing doctor
        doctor_id = data.get('doctor_id')
        if doctor_id:
            doctor_rel_query = """
            MATCH (d:Doctor {id: $doctor_id})
            MATCH (diag:Diagnosis {id: $diagnosis_id})
            MERGE (d)-[:DIAGNOSED]->(diag)
            """
            tx.run(doctor_rel_query, doctor_id=doctor_id, diagnosis_id=data['id'])
            relationships_created += 1
        
        return {"entity_id": data['id'], "type": "Diagnosis", "relationships_created": relationships_created}
    
    def _create_medication(self, tx, data):
        """Create Medication node and relationships"""
        query = """
        CREATE (med:Medication {
            id: $id,
            medication_name: $medication_name,
            dosage: $dosage,
            frequency: $frequency,
            indication: $indication,
            prescribed_date: $prescribed_date,
            quantity: $quantity,
            refills: $refills,
            status: $status,
            created_at: $timestamp
        })
        RETURN med.id as entity_id
        """
        
        tx.run(query,
               id=data['id'],
                       medication_name=data.get('medication_name', ''),
                       dosage=data.get('dosage', ''),
                       frequency=data.get('frequency', ''),
                       indication=data.get('indication', ''),
                       prescribed_date=data.get('prescribed_date', ''),
                       quantity=data.get('quantity', 0),
                       refills=data.get('refills', 0),
                       status=data.get('status', ''),
                       timestamp=datetime.utcnow().isoformat())
        
        relationships_created = 0
        
        # Create relationship to patient
        patient_id = data.get('patient_id')
        if patient_id:
            patient_rel_query = """
            MATCH (p:Patient {id: $patient_id})
            MATCH (med:Medication {id: $medication_id})
            MERGE (p)-[:PRESCRIBED {prescribed_date: $prescribed_date}]->(med)
            """
            tx.run(patient_rel_query, 
                  patient_id=patient_id, 
                  medication_id=data['id'],
                  prescribed_date=data.get('prescribed_date', ''))
            relationships_created += 1
        
        # Create relationship to prescribing doctor
        doctor_id = data.get('prescribing_doctor_id')
        if doctor_id:
            doctor_rel_query = """
            MATCH (d:Doctor {id: $doctor_id})
            MATCH (med:Medication {id: $medication_id})
            MERGE (d)-[:PRESCRIBED {date: $prescribed_date}]->(med)
            """
            tx.run(doctor_rel_query, 
                  doctor_id=doctor_id, 
                  medication_id=data['id'],
                  prescribed_date=data.get('prescribed_date', ''))
            relationships_created += 1
        
        return {"entity_id": data['id'], "type": "Medication", "relationships_created": relationships_created}
    
    def _create_procedure(self, tx, data):
        """Create Procedure node and relationships"""
        query = """
        CREATE (proc:Procedure {
            id: $id,
            cpt_code: $cpt_code,
            procedure_name: $procedure_name,
            procedure_type: $procedure_type,
            procedure_date: $procedure_date,
            duration_minutes: $duration_minutes,
            status: $status,
            cost: $cost,
            created_at: $timestamp
        })
        RETURN proc.id as entity_id
        """
        
        tx.run(query,
               id=data['id'],
                       cpt_code=data.get('cpt_code', ''),
                       procedure_name=data.get('procedure_name', ''),
                       procedure_type=data.get('procedure_type', ''),
                       procedure_date=data.get('procedure_date', ''),
                       duration_minutes=data.get('duration_minutes', 0),
                       status=data.get('status', ''),
                       cost=data.get('cost', 0.0),
                       timestamp=datetime.utcnow().isoformat())
        
        relationships_created = 0
        
        # Create relationship to patient
        patient_id = data.get('patient_id')
        if patient_id:
            patient_rel_query = """
            MATCH (p:Patient {id: $patient_id})
            MATCH (proc:Procedure {id: $procedure_id})
            MERGE (p)-[:UNDERWENT {date: $procedure_date, cost: $cost}]->(proc)
            """
            tx.run(patient_rel_query, 
                  patient_id=patient_id, 
                  procedure_id=data['id'],
                  procedure_date=data.get('procedure_date', ''),
                  cost=data.get('cost', 0.0))
            relationships_created += 1
        
        # Create relationship to performing doctor
        doctor_id = data.get('performing_doctor_id')
        if doctor_id:
            doctor_rel_query = """
            MATCH (d:Doctor {id: $doctor_id})
            MATCH (proc:Procedure {id: $procedure_id})
            MERGE (d)-[:PERFORMED {date: $procedure_date}]->(proc)
            """
            tx.run(doctor_rel_query, 
                  doctor_id=doctor_id, 
                  procedure_id=data['id'],
                  procedure_date=data.get('procedure_date', ''))
            relationships_created += 1
        
        # Create relationship to hospital
        hospital_id = data.get('hospital_id')
        if hospital_id:
            hospital_rel_query = """
            MATCH (h:Hospital {id: $hospital_id})
            MATCH (proc:Procedure {id: $procedure_id})
            MERGE (proc)-[:PERFORMED_AT]->(h)
            """
            tx.run(hospital_rel_query, hospital_id=hospital_id, procedure_id=data['id'])
            relationships_created += 1
        
        return {"entity_id": data['id'], "type": "Procedure", "relationships_created": relationships_created}
    
    def _create_generic_entity(self, tx, data):
        """Create generic entity for unknown types"""
        entity_type = data.get('type', 'Unknown').title()
        
        # Build dynamic property setting
        properties = {}
        for key, value in data.items():
            if key not in ['type']:
                properties[key] = value
        
        properties['created_at'] = datetime.utcnow().isoformat()
        
        query = f"""
        CREATE (e:{entity_type} $properties)
        RETURN e.id as entity_id
        """
        
        tx.run(query, properties=properties)
        return {"entity_id": data.get('id', str(uuid.uuid4())), "type": entity_type, "relationships_created": 0}
    
    def get_healthcare_statistics(self):
        """Get comprehensive healthcare statistics"""
        with self.driver.session(database=self.database) as session:
            # Node counts
            node_query = """
            CALL db.labels() YIELD label
            CALL apoc.cypher.run('MATCH (n:' + label + ') RETURN count(n) as count', {}) YIELD value
            RETURN label, value.count as count
            ORDER BY value.count DESC
            """
            
            try:
                node_result = session.run(node_query)
                node_stats = [{"entity_type": record["label"], "count": record["count"]} 
                             for record in node_result]
            except Exception:
                # Fallback if APOC not available
                basic_query = """
                MATCH (n)
                RETURN labels(n)[0] as entity_type, count(*) as count
                ORDER BY count DESC
                """
                node_result = session.run(basic_query)
                node_stats = [{"entity_type": record["entity_type"], "count": record["count"]} 
                             for record in node_result]
            
            # Relationship counts
            rel_query = """
            MATCH ()-[r]->()
            RETURN type(r) as relationship_type, count(r) as count
            ORDER BY count DESC
            """
            rel_result = session.run(rel_query)
            rel_stats = [{"relationship_type": record["relationship_type"], "count": record["count"]} 
                        for record in rel_result]
            
            # Total counts
            total_nodes = sum(stat['count'] for stat in node_stats)
            total_relationships = sum(stat['count'] for stat in rel_stats)
            
            return {
                "total_nodes": total_nodes,
                "total_relationships": total_relationships,
                "node_statistics": node_stats,
                "relationship_statistics": rel_stats
            }
    
    def get_sample_graph_data(self, limit=20):
        """Get sample data for visualization"""
        with self.driver.session(database=self.database) as session:
            query = """
            MATCH (p:Patient)-[r]->(connected)
            WITH p, r, connected
            LIMIT $limit
            RETURN p.id as patient_id, p.name as patient_name,
                   type(r) as relationship,
                   labels(connected)[0] as connected_type,
                   connected.id as connected_id,
                   CASE 
                     WHEN 'Doctor' IN labels(connected) THEN connected.name
                     WHEN 'Diagnosis' IN labels(connected) THEN connected.description
                     WHEN 'Medication' IN labels(connected) THEN connected.medication_name
                     WHEN 'Procedure' IN labels(connected) THEN connected.procedure_name
                     ELSE connected.name
                   END as connected_name
            """
            
            result = session.run(query, limit=limit)
            return [{"patient_id": r["patient_id"], 
                    "patient_name": r["patient_name"],
                    "relationship": r["relationship"],
                    "connected_type": r["connected_type"],
                    "connected_id": r["connected_id"],
                    "connected_name": r["connected_name"]} for r in result]
