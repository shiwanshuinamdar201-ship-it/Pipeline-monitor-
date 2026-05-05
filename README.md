# 🌊 Enterprise Data Pipeline Monitor

A real-time monitoring dashboard for data streaming pipelines, simulating **Kafka → Apache Spark → Data Warehouse** architecture with live telemetry and anomaly detection.

## 📋 Overview

This project demonstrates a complete streaming data pipeline with:
- **Kafka Producer Simulation**: Generates real-time user activity events
- **Apache Spark Streaming Processor**: Processes micro-batches of data with aggregation
- **Interactive Dashboard**: Real-time visualization using Streamlit and Plotly
- **Anomaly Detection**: Automatic detection of backpressure and pipeline issues
- **Live Metrics**: Throughput (msg/s) and latency monitoring with delta indicators

## 🎯 Key Features

✨ **Real-Time Monitoring**
- Live throughput and latency metrics
- Dual-axis time-series visualization
- Status indicators with color-coding

📊 **Data Pipeline Simulation**
- Kafka topic: `user_activity` with realistic event types
- Spark micro-batch processing with aggregations
- Simulated database writes to Delta Lake

🚨 **Anomaly Detection**
- Automatic detection of backpressure scenarios
- Visual alerts with delta calculations
- Healthy state monitoring

📈 **Production-Ready Dashboard**
- Wide responsive layout
- Optimized performance metrics
- Professional styling with Plotly

## 🏗️ Architecture

```
Pipeline Monitor
├── Kafka Producer (Simulated)
│   └── Generates: page_view, add_to_cart, purchase, login events
├── Spark Streaming (Simulated)
│   └── Processes: Micro-batches, Aggregations, DW writes
└── Streamlit Dashboard
    └── Real-time visualizations, metrics, alerts
```

## 📦 Installation

### Prerequisites
- Python 3.8+
- pip or conda

### Steps

1. **Clone the repository**
```bash
git clone https://github.com/shiwanshuinamdar201-ship-it/Pipeline-monitor-.git
cd Pipeline-monitor-
```

2. **Create a virtual environment**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

## 🚀 Usage

### Run the Streamlit Dashboard
```bash
streamlit run app.py
```
The dashboard will open in your browser at `http://localhost:8501`

### Run Producer Simulation
```bash
python src/kafka_producer.py
```

### Run Spark Processor Simulation
```bash
python src/spark_processor.py
```

## 📁 Project Structure

```
Pipeline-monitor/
├── app.py                    # Main Streamlit dashboard
├── requirements.txt          # Project dependencies
├── config.py                 # Configuration settings
├── README.md                 # Project documentation
├── LICENSE                   # MIT License
├── .gitignore               # Git ignore rules
│
├── src/
│   ├── kafka_producer.py    # Kafka producer simulation
│   ├── spark_processor.py   # Spark streaming processor
│   └── utils.py             # Utility functions
│
└── notebooks/
    └── Pipeline_Analysis.ipynb  # Data analysis notebook
```

## 🔧 Configuration

Edit `config.py` to customize:
- Kafka topic name
- Event generation frequency
- Anomaly detection thresholds
- Dashboard refresh rate
- Metric thresholds

## 📊 Metrics Explained

| Metric | Description | Normal Range |
|--------|-------------|--------------|
| **Kafka Throughput** | Messages produced per second | 4500-5500 msg/s |
| **Spark Latency** | Processing delay in milliseconds | 35-55 ms |
| **Status** | Pipeline health indicator | GREEN/RED |

## 🚨 Anomaly Detection

The system automatically detects backpressure conditions:
- **Throughput drops** to 400-600 msg/s (normal: 4500-5500)
- **Latency increases** to 250-350 ms (normal: 35-55)
- Visual indicators change to 🔴 **BACKPRESSURE DETECTED**

This helps identify:
- Consumer lag
- Resource constraints
- Network bottlenecks
- Downstream service issues

## 🎓 Learning Outcomes

This project demonstrates practical knowledge of:
- ✅ Stream processing concepts (Kafka, Spark)
- ✅ Real-time data visualization (Plotly, Streamlit)
- ✅ Data engineering patterns (micro-batching, aggregations)
- ✅ Monitoring and observability
- ✅ Python best practices (logging, error handling, documentation)
- ✅ Dashboard development and UX design

## 💡 Use Cases

1. **Learning Stream Processing**: Understand Kafka-Spark-DW architecture
2. **Portfolio Project**: Showcase data engineering skills
3. **Monitoring Dashboard Template**: Use as a base for real pipelines
4. **Teaching Tool**: Demonstrate streaming concepts to learners

## 🔮 Future Enhancements

- [ ] Integration with real Kafka cluster
- [ ] Apache Spark cluster connectivity
- [ ] Database connector (PostgreSQL, Delta Lake)
- [ ] Historical data analysis with aggregations
- [ ] Slack/Email alerting for anomalies
- [ ] Multi-tenant monitoring dashboard
- [ ] Custom metric definitions

## 📝 Requirements

```
pandas==2.2.0
streamlit==1.31.0
plotly==5.18.0
numpy==1.26.3
jupyter==1.0.0
matplotlib==3.8.2
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🤝 Contributing

Contributions are welcome! Feel free to:
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/improvement`)
3. Commit changes (`git commit -m 'Add improvement'`)
4. Push to branch (`git push origin feature/improvement`)
5. Open a Pull Request

## 📧 Contact & Support

**Author**: Shiwanshu Inamdar  
**GitHub**: [@shiwanshuinamdar201-ship-it](https://github.com/shiwanshuinamdar201-ship-it)  
**Project Link**: [Pipeline Monitor](https://github.com/shiwanshuinamdar201-ship-it/Pipeline-monitor-)

For questions or issues, please open an issue on GitHub.

## 🎖️ Acknowledgments

- Apache Kafka & Apache Spark documentation
- Streamlit documentation and examples
- Plotly visualization library
- Data engineering best practices from industry standards

---

**⭐ If you find this project helpful, please star it on GitHub!**
