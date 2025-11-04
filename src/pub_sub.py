from google.cloud import pubsub_v1
import json
import random
import time
from datetime import datetime

# ======== CONFIGURATION ========
project_id = "ai-health-risk-system"        # replace with your GCP project ID
topic_id = "health_stream_topic"      # main streaming topic

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# ======== SCHEMA-BASED SAMPLE GENERATOR ========
def generate_patient_record():
    conditions = ["Cardiac Risk", "Hypertension", "Diabetes", "Asthma", "Obesity"]
    name_list = ["venkat", "subharao", "ganesan", "rahul", "singh", "Takur","Pandya","Sharama","kumar","goud","nayar"]
    gender_list = ["Male", "Female"]
    
    record = {
        "Patient_ID": f"P{random.randint(1000,9999)}",
        "Name": random.choice(name_list),
        "Age": random.randint(25, 85),
        "Gender": random.choice(gender_list),
        "HeartRate": round(random.uniform(60, 130), 2),
        "BloodPressure": round(random.uniform(110, 180), 2),
        "Temperature": round(random.uniform(97.0, 100.5), 2),
        "MedicalCondition": random.choice(conditions),
        "RiskScore": round(random.uniform(0.1, 1.0), 2),
        "DateOfAdmission": datetime.utcnow().isoformat() + "Z"
    }
    return record

# ======== PUBLISH LOOP ========
def publish_streaming_data():
    while True:
        record = generate_patient_record()
        message_json = json.dumps(record).encode("utf-8")
        
        future = publisher.publish(topic_path, message_json)
        print(f"✅ Published message ID: {future.result()}")
        print(json.dumps(record, indent=2))
        
        # send message every few seconds
        time.sleep(5)

if __name__ == "__main__":
    publish_streaming_data()
