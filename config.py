"""
Configuration settings for Pipeline Monitor Application

This module contains all configurable parameters for the streaming
pipeline simulation and monitoring dashboard.
"""

# ============================================================================
# KAFKA CONFIGURATION
# ============================================================================
KAFKA_CONFIG = {
    "topic": "user_activity",
    "partition": 0,
    "brokers": ["localhost:9092"],
    "event_types": ["page_view", "add_to_cart", "purchase", "login"],
    "user_count": 1000,
    "event_generation_interval": 0.5,  # seconds
}

# ============================================================================
# SPARK STREAMING CONFIGURATION
# ============================================================================
SPARK_CONFIG = {
    "app_name": "Pipeline_Monitor",
    "batch_duration": 2,  # seconds
    "checkpoint_dir": "./checkpoints",
    "watermark_delay": "10 seconds",
}

# ============================================================================
# MONITORING & ANOMALY DETECTION
# ============================================================================
MONITORING_CONFIG = {
    # Throughput thresholds (messages per second)
    "normal_throughput_mean": 5000,
    "normal_throughput_std": 500,
    "anomaly_throughput_mean": 500,  # During backpressure
    "anomaly_throughput_std": 100,
    
    # Latency thresholds (milliseconds)
    "normal_latency_mean": 45,
    "normal_latency_std": 10,
    "anomaly_latency_mean": 300,
    "anomaly_latency_std": 50,
    
    # Anomaly detection window
    "anomaly_start_step": 10,
    "anomaly_end_step": 20,
    "anomaly_probability": 0.8,  # 80% chance to show anomaly in detection window
}

# ============================================================================
# DASHBOARD CONFIGURATION
# ============================================================================
DASHBOARD_CONFIG = {
    "page_title": "Data Pipeline Monitor",
    "page_layout": "wide",
    "update_interval": 1,  # seconds
    "max_history_points": 30,  # Keep last N data points
    "refresh_rate": 1000,  # milliseconds between refreshes
}

# ============================================================================
# DATA WAREHOUSE CONFIGURATION
# ============================================================================
DW_CONFIG = {
    "connection_type": "simulated",  # "simulated", "postgresql", "delta"
    "batch_mode": "append",
    "checkpoint_enabled": True,
}

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================
LOGGING_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "log_file": "logs/pipeline_monitor.log",
    "enable_file_logging": True,
}

# ============================================================================
# PIPELINE METRICS
# ============================================================================
METRICS = {
    "throughput": {
        "name": "Kafka Throughput (msg/s)",
        "unit": "messages/second",
        "color": "blue",
    },
    "latency": {
        "name": "Spark Latency (ms)",
        "unit": "milliseconds",
        "color": "red",
    },
    "status": {
        "healthy": "🟢 HEALTHY",
        "warning": "🟡 WARNING",
        "critical": "🔴 BACKPRESSURE DETECTED",
    },
}

# ============================================================================
# SIMULATION PARAMETERS
# ============================================================================
SIMULATION_CONFIG = {
    "total_events_to_simulate": 100,
    "payload_size_range": (100, 5000),  # bytes
    "event_delay_simulation": 0.01,  # seconds
}
