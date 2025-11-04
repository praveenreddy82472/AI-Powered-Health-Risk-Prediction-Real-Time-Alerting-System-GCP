"""
streaming_pipeline_alerts.py

- Reads JSON from Pub/Sub
- Computes RiskScore
- Publishes alerts to Pub/Sub if threshold exceeded
"""

import argparse, json, uuid
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions, WorkerOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub, WriteToPubSub

def safe_parse(element):
    try:
        return json.loads(element.decode("utf-8"))
    except Exception:
        return None

def enrich_and_compute(row, threshold=0.8):
    now = datetime.utcnow().isoformat() + "Z"
    row["_ingestion_id"] = str(uuid.uuid4())
    row["_ingestion_ts"] = now
    row["EventTimestamp"] = row.get("DateOfAdmission") or now
    hr = float(row.get("HeartRate") or 0)
    bp = float(row.get("BloodPressure") or 0)
    age = float(row.get("Age") or 0)
    condition = (row.get("MedicalCondition") or "").lower()

    risk = 0.0
    if hr > 120: risk += 0.3
    if bp > 160: risk += 0.2
    if age > 65: risk += 0.1
    if any(c in condition for c in ["cardiac", "hypertension", "diabetes"]): risk += 0.3

    row["RiskScore"] = min(risk, 1.0)
    row["_alert"] = row["RiskScore"] >= threshold
    return row

def prepare_alert(row):
    return json.dumps({
        "Patient_ID": row.get("Patient_ID"),
        "RiskScore": row["RiskScore"],
        "EventTimestamp": row["EventTimestamp"],
        "ingestion_id": row["_ingestion_id"]
    }).encode("utf-8")

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--input_subscription", required=True)
    parser.add_argument("--alerts_topic", required=True)
    parser.add_argument("--staging_location", required=True)
    parser.add_argument("--temp_location", required=True)
    parser.add_argument("--job_name", required=True)
    parser.add_argument("--alert_threshold", type=float, default=0.8)
    parser.add_argument("--num_workers", type=int, default=2)
    parser.add_argument("--max_num_workers", type=int, default=5)
    parser.add_argument("--worker_machine_type", default="n1-standard-2")

    known_args, pipeline_args = parser.parse_known_args(argv)
    options = PipelineOptions(pipeline_args, save_main_session=True, experiments=["enable_streaming_engine"])
    options.view_as(StandardOptions).streaming = True
    gcloud = options.view_as(GoogleCloudOptions)
    gcloud.project = known_args.project
    gcloud.region = known_args.region
    gcloud.staging_location = known_args.staging_location
    gcloud.temp_location = known_args.temp_location
    gcloud.job_name = known_args.job_name

    worker_opts = options.view_as(WorkerOptions)
    worker_opts.num_workers = known_args.num_workers
    worker_opts.max_num_workers = known_args.max_num_workers
    worker_opts.machine_type = known_args.worker_machine_type
    worker_opts.autoscaling_algorithm = "THROUGHPUT_BASED"

    with beam.Pipeline(options=options) as p:
        messages = p | "ReadFromPubSub" >> ReadFromPubSub(subscription=known_args.input_subscription)
        parsed = messages | "ParseJSON" >> beam.Map(safe_parse) | "FilterValid" >> beam.Filter(lambda x: x is not None)
        processed = parsed | "EnrichAndComputeRisk" >> beam.Map(enrich_and_compute, threshold=known_args.alert_threshold)
        alerts = processed | "FilterAlerts" >> beam.Filter(lambda x: x["_alert"]) | "PrepareAlertJSON" >> beam.Map(prepare_alert)
        alerts | "WriteAlertsToPubSub" >> WriteToPubSub(topic=known_args.alerts_topic)

if __name__ == "__main__":
    run()
