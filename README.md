# 🩺 AI-Powered Health Risk Prediction System (Real-Time GCP Pipeline)

## 📖 Project Overview
The **AI-Powered Health Risk Prediction System** is a real-time healthcare analytics pipeline built entirely on **Google Cloud Platform (GCP)**.  
It continuously streams simulated patient health data, performs **real-time monitoring, alerting, transformation, and machine learning-based prediction**, and delivers insights through **Looker dashboards**.

This project demonstrates how an enterprise can build **low-latency, scalable, and intelligent healthcare monitoring systems** using GCP’s fully managed services.

---

## 🚀 Key Objectives
- Stream live health telemetry data for patients.  
- Detect anomalies and generate **automated alerts** when risk levels exceed thresholds.  
- Maintain a **modern data-lakehouse architecture** with Bronze → Silver → Gold layers.  
- Train **ML models in BigQuery ML** to predict patient risk trends.  
- Deliver real-time reports and insights via **Looker dashboards**.

---

---

### 🧩 Design Evolution & Learnings

While building this system, I learned that **real-time and batch orchestration require different execution models** on GCP.  
Initially, I wanted to run everything — ingestion, transformation, validation, and alerts — in a single unified pipeline.  
But during testing, I discovered that this approach was not scalable or maintainable in a real-time production setup.

---

#### ⚠️ Challenge 1: One Job Can’t Handle It All
At first, I tried using a **single Dataflow pipeline** to perform:
- Ingestion from Pub/Sub  
- Validation and transformation  
- Threshold-based alerting  

However, combining all of these caused operational issues.  
The job became complex, error-prone, and **not compatible with continuous production streaming** because each operation had its own frequency and latency needs.

---

#### ⚙️ Challenge 2: Latency & Bottlenecks
When all processes were handled in one Dataflow job, it created:
- **High data lag** and inconsistent watermarking  
- **System bottlenecks**, as transformations waited on alert logic  
- **Scalability issues**, since streaming jobs need to run continuously while transformations are periodic  

From this, I learned the key architectural principle:  
> “In real-time pipelines, alerting and transformations should be handled by separate, specialized jobs.”

---

#### 🏗️ Solution: Medallion Architecture (Bronze → Silver → Gold)
To address these challenges, I implemented a **Medallion Data Architecture** for BigQuery:
- 🥉 **Bronze (Raw):** Native data streamed from Pub/Sub into BigQuery using **Dataflow Job 1**  
- 🥈 **Silver:** Cleaned, validated, and deduplicated data using **scheduled SQL queries** in **Cloud Composer**  
- 🥇 **Gold:** Fully curated, analytics-ready data optimized for **ML training** and **Looker dashboards**

This separation provided **data lineage, maintainability, and reliability**, allowing each process to run independently.

---

#### ⚡ Challenge 3: Orchestration Compatibility
Initially, I thought to orchestrate all pipelines (Pub/Sub, Dataflow, BigQuery) within **Cloud Composer**.  
But I soon realized that:
- **Pub/Sub** and **Dataflow** are **streaming and continuously running** services  
- **Cloud Composer (Airflow)** is designed for **scheduled, event-based orchestration**  

This means **continuous pipelines** don’t fit into Composer’s DAG execution cycle.

So instead:
- I used **Flex Templates** to deploy Dataflow jobs that run continuously outside of Composer.  
- Composer now focuses on **batch orchestration** — scheduling SQL scripts, updating the Silver and Gold tables, and triggering **ML training and prediction workflows**.

---

#### 🧠 Final Insight: Balancing Real-Time and Batch
The final system balances **real-time streaming** and **scheduled batch orchestration**:

| Layer | Technology | Purpose |
|--------|-------------|----------|
| **Streaming Layer** | Pub/Sub → Dataflow | Real-time ingestion & alert pipeline |
| **Storage Layer** | BigQuery (Bronze) | Central raw data lake |
| **Transformation Layer** | Cloud Composer (SQL DAGs) | Cleansing, validation & enrichment |
| **ML Layer** | BigQuery ML | Model training & predictions |
| **Alert Layer** | Pub/Sub → Cloud Function → SendGrid | Real-time alerts via email |
| **Visualization Layer** | Looker | Dashboard insights via materialized views |

By decoupling these layers, the system now supports **low-latency alerts**, **high-quality analytics**, and **scalable model retraining**, achieving a truly **real-time AI-powered health monitoring system**.

---


## 🧠 Architecture Story (Step-by-Step Flow)

### 1. Data Simulation & Ingestion
A Python script continuously generates **synthetic patient data** (Age, HeartRate, BloodPressure, Temperature, etc.).  
This data is published to a **Pub/Sub topic**, acting as the ingestion gateway for the system.

### 2. Parallel Dataflow Pipelines
Two **Dataflow jobs** (Apache Beam pipelines) process the streaming data:
- **Dataflow Job 1:** Reads JSON messages from Pub/Sub and writes them into **BigQuery (Bronze layer)**.
- **Dataflow Job 2:** Reads the same stream, applies **threshold logic** on the `RiskScore` field, and if it exceeds a defined limit, pushes it to another **Pub/Sub alert topic**.

### 3. Real-Time Alerting
The alert topic triggers a **Cloud Function**, which sends an automated email via **SendGrid API** whenever a high-risk patient is detected.  
Alerts reach the configured Gmail inbox within seconds — enabling proactive intervention.

### 4. Data Transformation Layers (Bronze → Silver → Gold)
Data is stored and refined in **BigQuery** across three layers:
- 🥉 **Bronze (Raw):** All ingested JSON records.  
- 🥈 **Silver:** Cleaned and validated data via scheduled SQL queries in **Cloud Composer**.  
- 🥇 **Gold:** Fully curated data, optimized for ML and analytics.

### 5. Machine Learning in BigQuery ML
Using the **Gold** table, the system trains a **classification/regression model** (e.g., XGBoost or Logistic Regression) to predict patient risk scores for upcoming days or weeks.  
Predictions and accuracy metrics are stored back into BigQuery for continuous evaluation.

### 6. Insights & Visualization
**Materialized Views** built on top of the Gold and prediction tables provide near real-time insights for dashboards.  
These are connected to **Looker Studio**, giving stakeholders a clear view of patient trends, risk levels, and forecasted outcomes.

---

## 🏗️ Cloud Architecture Diagram
![Architecture Diagram](https://github.com/praveenreddy82472/AI-Powered-Health-Risk-Prediction-Real-Time-Alerting-System-GCP-/blob/main/Images/final_archi.png)

---

## ☁️ Tech Stack

| Category | Services / Tools |
|-----------|------------------|
| **Data Ingestion** | Pub/Sub |
| **Stream Processing** | Dataflow (Apache Beam) |
| **Storage & Warehousing** | BigQuery |
| **Orchestration** | Cloud Composer (Airflow) |
| **Alerting** | Cloud Functions, SendGrid, Gmail |
| **Machine Learning** | BigQuery ML |
| **Visualization** | Looker Studio |
| **Language** | Python, SQL |

---

## ⚙️ Deployment Steps
1. Create Pub/Sub topics & subscriptions.  
2. Launch Dataflow streaming pipelines using Flex templates.  
3. Deploy Cloud Function for email alerts via SendGrid.  
4. Create BigQuery datasets and SQL scripts for Silver and Gold transformations.  
5. Schedule these transformations with Cloud Composer DAGs.  
6. Train and evaluate ML models in BigQuery ML.  
7. Connect **Materialized Views** to Looker for real-time dashboards.

---

## 📊 Real-Time Capabilities
- End-to-end latency: < 5 seconds from ingestion to alert delivery.  
- Incremental transformations ensure only **new data** is processed.  
- Materialized Views auto-refresh for **live analytics**.  

---

## 📈 Outcome
This system provides **end-to-end automation** — from streaming patient vitals to predictive insights — demonstrating a **real-time AI-driven data engineering pipeline** on GCP.

---

## 🧩 Summary (For Portfolio / Resume)
> Built a real-time AI health monitoring pipeline using GCP (Pub/Sub, Dataflow, BigQuery, Cloud Composer, Looker) that detects high-risk patients and predicts health trends via BigQuery ML.

---

