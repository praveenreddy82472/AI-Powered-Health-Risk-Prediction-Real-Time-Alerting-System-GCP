"""
streaming_pipeline_raw.py

- Reads JSON from Pub/Sub
- Lightweight parsing + enrich ingestion/processing timestamps
- Writes raw data to BigQuery only
"""

import argparse, json, uuid
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions, WorkerOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

def safe_parse(element):
    try:
        return json.loads(element.decode("utf-8"))
    except Exception:
        return None

def enrich_row(row):
    now = datetime.utcnow().isoformat() + "Z"
    row["_ingestion_id"] = str(uuid.uuid4())
    row["_ingestion_ts"] = now
    row["EventTimestamp"] = row.get("DateOfAdmission") or now
    row["_processing_ts"] = datetime.utcnow().isoformat() + "Z"
    return row

def to_bq_row(d):
    payload = {k: v for k, v in d.items() if not k.startswith("_")}
    return {
        "Patient_ID": d.get("Patient_ID"),
        "EventTimestamp": d.get("EventTimestamp"),
        "payload": json.dumps(payload),
        "IngestionID": d.get("_ingestion_id"),
        "IngestionTS": d.get("_ingestion_ts"),
        "ProcessingTS": d.get("_processing_ts")
    }

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--input_subscription", required=True)
    parser.add_argument("--bq_dataset", required=True)
    parser.add_argument("--raw_table", required=True)
    parser.add_argument("--staging_location", required=True)
    parser.add_argument("--temp_location", required=True)
    parser.add_argument("--job_name", required=True)
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
        enriched = parsed | "EnrichRow" >> beam.Map(enrich_row)
        enriched | "ToBQRow" >> beam.Map(to_bq_row) | "WriteRawToBQ" >> WriteToBigQuery(
            table=f"{known_args.bq_dataset}.{known_args.raw_table}",
            schema={
                "fields": [
                    {"name": "Patient_ID", "type": "STRING"},
                    {"name": "EventTimestamp", "type": "TIMESTAMP"},
                    {"name": "payload", "type": "STRING"},
                    {"name": "IngestionID", "type": "STRING"},
                    {"name": "IngestionTS", "type": "TIMESTAMP"},
                    {"name": "ProcessingTS", "type": "TIMESTAMP"}
                ]
            },
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            method=WriteToBigQuery.Method.STREAMING_INSERTS
        )

if __name__ == "__main__":
    run()
