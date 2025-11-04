import functions_framework
import base64
import json
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

SENDGRID_API_KEY = os.environ.get("SENDGRID_API_KEY")
FROM_EMAIL = "tumatipraveenreddy18@gmail.com"
TO_EMAIL = os.environ.get("ALERT_TO_EMAIL", "tumatipraveenreddy18@gmail.com")

@functions_framework.cloud_event
def pubsub_alert(cloud_event):
    data = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")
    try:
        payload = json.loads(data)
    except:
        payload = {"raw": data}

    subject = f"ALERT: Patient {payload.get('Patient_ID')} high risk {payload.get('RiskScore')}"
    body = f"Patient {payload.get('Patient_ID')} risk {payload.get('RiskScore')} at {payload.get('EventTimestamp')}\n\nIngestion ID: {payload.get('ingestion_id')}"
    message = Mail(from_email=FROM_EMAIL, to_emails=TO_EMAIL, subject=subject, html_content=body)

    sg = SendGridAPIClient(SENDGRID_API_KEY)
    response = sg.send(message)
    print("SendGrid response:", response.status_code)
