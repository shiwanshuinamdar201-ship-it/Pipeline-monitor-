"""
Utility Functions for Pipeline Monitor

This module contains helper functions and utilities used across
the pipeline monitoring application.

Author: Shiwanshu Inamdar
Date: 2024
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from config import LOGGING_CONFIG

def setup_logging():
    """Set up application-wide logging configuration."""
    # Create logs directory if it doesn't exist
    log_dir = os.path.dirname(LOGGING_CONFIG['log_file'])
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, LOGGING_CONFIG['level']),
        format=LOGGING_CONFIG['format'],
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(LOGGING_CONFIG['log_file']) if LOGGING_CONFIG['enable_file_logging'] else logging.NullHandler()
        ]
    )

def create_sample_data_file(filepath: str = "data/sample_events.json", num_events: int = 100):
    """
    Create a sample data file with generated events for testing.

    Args:
        filepath: Path to save the sample data
        num_events: Number of events to generate
    """
    try:
        from src.kafka_producer import KafkaProducerSimulator

        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        producer = KafkaProducerSimulator()
        events = []

        for _ in range(num_events):
            event = producer.generate_event()
            events.append(event)

        # Save to file
        with open(filepath, 'w') as f:
            json.dump(events, f, indent=2, default=str)

        logging.info(f"Created sample data file: {filepath} with {num_events} events")

    except Exception as e:
        logging.error(f"Error creating sample data file: {e}")

def load_config_from_env() -> Dict[str, Any]:
    """
    Load configuration overrides from environment variables.

    Returns:
        Dictionary of configuration overrides
    """
    overrides = {}

    # Kafka config overrides
    if os.getenv('KAFKA_TOPIC'):
        overrides['kafka_topic'] = os.getenv('KAFKA_TOPIC')
    if os.getenv('KAFKA_BROKERS'):
        overrides['kafka_brokers'] = os.getenv('KAFKA_BROKERS').split(',')

    # Dashboard config overrides
    if os.getenv('DASHBOARD_UPDATE_INTERVAL'):
        overrides['dashboard_update_interval'] = int(os.getenv('DASHBOARD_UPDATE_INTERVAL'))

    # Monitoring config overrides
    if os.getenv('ANOMALY_PROBABILITY'):
        overrides['anomaly_probability'] = float(os.getenv('ANOMALY_PROBABILITY'))

    return overrides

def validate_environment():
    """
    Validate that the environment is properly set up for the application.

    Returns:
        bool: True if environment is valid, False otherwise
    """
    issues = []

    # Check Python version
    import sys
    if sys.version_info < (3, 8):
        issues.append("Python 3.8+ required")

    # Check required packages
    required_packages = ['streamlit', 'pandas', 'plotly', 'numpy']
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            issues.append(f"Missing required package: {package}")

    # Check directories
    required_dirs = ['src', 'logs']
    for dir_name in required_dirs:
        if not os.path.exists(dir_name):
            try:
                os.makedirs(dir_name)
            except Exception as e:
                issues.append(f"Cannot create directory {dir_name}: {e}")

    if issues:
        logging.error("Environment validation failed:")
        for issue in issues:
            logging.error(f"  - {issue}")
        return False

    logging.info("Environment validation passed")
    return True

def format_bytes(bytes_value: int) -> str:
    """
    Format bytes into human-readable format.

    Args:
        bytes_value: Number of bytes

    Returns:
        Formatted string (e.g., "1.5 MB")
    """
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_value < 1024.0:
            return ".1f"
        bytes_value /= 1024.0
    return ".1f"

def calculate_throughput_metrics(events: list, time_window_seconds: int = 60) -> Dict[str, float]:
    """
    Calculate throughput metrics from a list of events.

    Args:
        events: List of event dictionaries with timestamps
        time_window_seconds: Time window for calculation

    Returns:
        Dictionary with throughput metrics
    """
    if not events:
        return {'events_per_second': 0.0, 'total_events': 0}

    try:
        # Sort events by timestamp
        sorted_events = sorted(events, key=lambda x: x.get('timestamp', datetime.now()))

        # Calculate time span
        start_time = sorted_events[0]['timestamp']
        end_time = sorted_events[-1]['timestamp']

        if isinstance(start_time, str):
            start_time = datetime.fromisoformat(start_time)
        if isinstance(end_time, str):
            end_time = datetime.fromisoformat(end_time)

        time_span_seconds = (end_time - start_time).total_seconds()
        if time_span_seconds <= 0:
            time_span_seconds = time_window_seconds

        events_per_second = len(events) / time_span_seconds

        return {
            'events_per_second': events_per_second,
            'total_events': len(events),
            'time_span_seconds': time_span_seconds
        }

    except Exception as e:
        logging.error(f"Error calculating throughput metrics: {e}")
        return {'events_per_second': 0.0, 'total_events': len(events)}

def export_metrics_to_csv(data: Dict[str, Any], filepath: str):
    """
    Export metrics data to CSV file.

    Args:
        data: Metrics data dictionary
        filepath: Path to save CSV file
    """
    try:
        import pandas as pd

        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Create directory if needed
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        # Save to CSV
        df.to_csv(filepath, index=False)
        logging.info(f"Metrics exported to: {filepath}")

    except Exception as e:
        logging.error(f"Error exporting metrics to CSV: {e}")

def get_system_info() -> Dict[str, Any]:
    """
    Get system information for diagnostics.

    Returns:
        Dictionary with system information
    """
    import platform
    import psutil

    try:
        return {
            'platform': platform.platform(),
            'python_version': platform.python_version(),
            'cpu_count': psutil.cpu_count(),
            'memory_total': psutil.virtual_memory().total,
            'memory_available': psutil.virtual_memory().available,
            'timestamp': datetime.now().isoformat()
        }
    except ImportError:
        # Fallback if psutil not available
        return {
            'platform': platform.platform(),
            'python_version': platform.python_version(),
            'timestamp': datetime.now().isoformat()
        }
    except Exception as e:
        logging.error(f"Error getting system info: {e}")
        return {'error': str(e)}

# Initialize logging when module is imported
setup_logging()