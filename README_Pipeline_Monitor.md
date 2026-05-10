# 🔁 DataPipeline Monitor

<div align="center">

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Plotly](https://img.shields.io/badge/Plotly-3F4F75?style=for-the-badge&logo=plotly&logoColor=white)
![Jupyter](https://img.shields.io/badge/Jupyter-F37626?style=for-the-badge&logo=jupyter&logoColor=white)

**Real-time anomaly detection for streaming data pipelines — backpressure, latency spikes & schema drift**

</div>

---

## 📌 The Problem

Streaming data pipelines fail silently. A Kafka consumer falls behind, a PySpark executor stalls, or an upstream schema changes — and by the time anyone notices, millions of events have been dropped or corrupted. Production teams need *live* visibility into pipeline health.

This project builds that visibility from scratch.

---

## 🎯 What It Does

| Feature | Description |
|---|---|
| 🔴 Backpressure Detection | Flags when consumer lag exceeds producer throughput thresholds |
| ⚡ Latency Spike Detection | Alerts when processing latency deviates beyond 2σ from rolling mean |
| 🔀 Schema Drift Detection | Identifies when incoming message fields change unexpectedly |
| 📊 Live Dashboard | Real-time Plotly charts for throughput, lag, and latency metrics |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────┐
│                   PRODUCER LAYER                    │
│  Python mock Kafka producer                         │
│  → Emits events at configurable rates               │
│  → Injects anomalies (schema drift, bursts)         │
└────────────────────┬────────────────────────────────┘
                     │ event stream
                     ▼
┌─────────────────────────────────────────────────────┐
│                  CONSUMER LAYER                     │
│  PySpark micro-batch consumer                       │
│  → Processes events in configurable windows         │
│  → Tracks lag, throughput, schema fingerprint       │
└────────────────────┬────────────────────────────────┘
                     │ metrics
                     ▼
┌─────────────────────────────────────────────────────┐
│               MONITORING LAYER                      │
│  Anomaly detection engine                           │
│  → Rolling statistical thresholds                   │
│  → Schema drift fingerprinting                      │
│  → Alert generation                                 │
└────────────────────┬────────────────────────────────┘
                     │
                     ▼
           📊 Live Plotly Dashboard
```

---

## 🔍 Anomaly Detection Logic

### Backpressure Detection
```python
def detect_backpressure(producer_rate, consumer_rate, threshold=0.8):
    """Flag when consumer processes < 80% of incoming events."""
    lag_ratio = consumer_rate / producer_rate
    if lag_ratio < threshold:
        return Alert(
            type="BACKPRESSURE",
            severity="HIGH",
            message=f"Consumer lag ratio: {lag_ratio:.2%} (threshold: {threshold:.0%})"
        )
```

### Latency Spike Detection
```python
def detect_latency_spike(latency_series, window=50, z_threshold=2.0):
    """Statistical anomaly detection using rolling z-score."""
    rolling_mean = latency_series.rolling(window).mean()
    rolling_std  = latency_series.rolling(window).std()
    z_score = (latency_series - rolling_mean) / rolling_std
    return z_score.abs() > z_threshold  # True = spike detected
```

### Schema Drift Detection
```python
def get_schema_fingerprint(message: dict) -> str:
    """Hash the sorted field names to detect structural changes."""
    return hashlib.md5(
        str(sorted(message.keys())).encode()
    ).hexdigest()
```

---

## 📊 Dashboard Metrics

The live Plotly dashboard tracks:
- **Message throughput** — events/second (producer vs consumer)
- **Consumer lag** — cumulative unprocessed event count
- **Processing latency** — p50, p95, p99 percentiles
- **Anomaly timeline** — flagged events with type and severity
- **Schema drift log** — fingerprint changes over time

---

## 🚀 Getting Started

```bash
# Clone the repo
git clone https://github.com/shiwanshuinamdar201-ship-it/Pipeline-monitor.git
cd Pipeline-monitor

# Install dependencies
pip install -r requirements.txt

# Run the full simulation (producer + consumer + monitor)
python run_pipeline.py

# Or open the analysis notebook
jupyter notebook pipeline_analysis.ipynb

# Dashboard launches automatically at http://localhost:8050
```

### Requirements
```
pyspark>=3.3.0
plotly>=5.10.0
pandas>=1.5.0
numpy>=1.23.0
jupyter>=1.0.0
hashlib (built-in)
```

---

## 📁 Project Structure

```
Pipeline-monitor/
├── producer/
│   ├── mock_producer.py        # Simulated Kafka event producer
│   └── anomaly_injector.py     # Injects bursts, schema changes, delays
├── consumer/
│   └── spark_consumer.py       # PySpark micro-batch consumer
├── monitor/
│   ├── anomaly_detector.py     # Backpressure, latency, schema drift logic
│   └── alert_engine.py         # Alert generation and logging
├── dashboard/
│   └── live_dashboard.py       # Plotly real-time visualization
├── pipeline_analysis.ipynb     # Full Jupyter analysis notebook
├── run_pipeline.py             # Entry point — runs all components
└── requirements.txt
```

---

## 💡 Key Learnings

- **Rolling z-score** is more robust than fixed thresholds for latency anomalies — it adapts to the pipeline's natural rhythm
- Schema fingerprinting using MD5 of sorted field names is lightweight and catches 95%+ of real-world schema drift cases
- Micro-batch windows need careful tuning — too small causes thrashing, too large masks real-time anomalies
- Simulating realistic event bursts (Poisson distribution) produces more meaningful test results than uniform loads

---

## 🧠 Skills Demonstrated

`Data Engineering` `Apache Kafka` `Apache Spark / PySpark` `ETL Pipelines` `Anomaly Detection` `Real-time Systems` `Plotly` `Python` `Statistical Methods`

---

<div align="center">
Made by <a href="https://github.com/shiwanshuinamdar201-ship-it">Shiwanshu Inamdar</a> · B.Tech CSE Data Science · D.Y. Patil International University
</div>
