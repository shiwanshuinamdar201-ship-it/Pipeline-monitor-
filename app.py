"""
Enterprise Data Pipeline Monitor Dashboard

A real-time monitoring dashboard for streaming data pipelines that simulates
Kafka → Apache Spark → Data Warehouse architecture with live telemetry
and anomaly detection capabilities.

Author: Shiwanshu Inamdar
Date: 2024
"""

import streamlit as st
import pandas as pd
import numpy as np
import time
import logging
from datetime import datetime
import plotly.graph_objects as go
from config import (
    DASHBOARD_CONFIG, MONITORING_CONFIG, METRICS,
    LOGGING_CONFIG
)

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOGGING_CONFIG['level']),
    format=LOGGING_CONFIG['format'],
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOGGING_CONFIG['log_file']) if LOGGING_CONFIG['enable_file_logging'] else logging.NullHandler()
    ]
)
logger = logging.getLogger(__name__)

def setup_page_config():
    """Configure Streamlit page settings and styling."""
    st.set_page_config(
        page_title=DASHBOARD_CONFIG['page_title'],
        layout=DASHBOARD_CONFIG['page_layout'],
        page_icon="🌊"
    )

    # Custom CSS for better styling
    st.markdown("""
    <style>
    .metric-container {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .status-healthy {
        color: #00cc44;
        font-weight: bold;
    }
    .status-warning {
        color: #ffaa00;
        font-weight: bold;
    }
    .status-critical {
        color: #ff4444;
        font-weight: bold;
    }
    </style>
    """, unsafe_allow_html=True)

def initialize_session_state():
    """Initialize Streamlit session state variables."""
    if 'data' not in st.session_state:
        st.session_state.data = pd.DataFrame(columns=['Time', 'Throughput', 'Latency', 'Status'])
    if 'iteration' not in st.session_state:
        st.session_state.iteration = 0

def generate_metrics(iteration: int) -> tuple:
    """
    Generate simulated throughput and latency metrics with anomaly detection.

    Args:
        iteration: Current simulation iteration

    Returns:
        tuple: (throughput, latency, status)
    """
    try:
        # Check if we're in anomaly window
        is_anomaly_period = (
            MONITORING_CONFIG['anomaly_start_step'] <= iteration <= MONITORING_CONFIG['anomaly_end_step'] and
            np.random.random() < MONITORING_CONFIG['anomaly_probability']
        )

        if is_anomaly_period:
            # Anomaly: Backpressure detected
            throughput = np.random.normal(
                MONITORING_CONFIG['anomaly_throughput_mean'],
                MONITORING_CONFIG['anomaly_throughput_std']
            )
            latency = np.random.normal(
                MONITORING_CONFIG['anomaly_latency_mean'],
                MONITORING_CONFIG['anomaly_latency_std']
            )
            status = METRICS['status']['critical']
            logger.warning(f"Anomaly detected at iteration {iteration}: Throughput={throughput:.1f}, Latency={latency:.1f}")
        else:
            # Normal operation
            throughput = np.random.normal(
                MONITORING_CONFIG['normal_throughput_mean'],
                MONITORING_CONFIG['normal_throughput_std']
            )
            latency = np.random.normal(
                MONITORING_CONFIG['normal_latency_mean'],
                MONITORING_CONFIG['normal_latency_std']
            )
            status = METRICS['status']['healthy']

        # Ensure positive values
        throughput = max(0, throughput)
        latency = max(0, latency)

        return throughput, latency, status

    except Exception as e:
        logger.error(f"Error generating metrics: {e}")
        return 5000, 45, METRICS['status']['warning']

def create_metrics_display(throughput: float, latency: float, status: str, iteration: int):
    """Create and display the metrics dashboard."""
    col1, col2, col3 = st.columns(3)

    # Throughput metric
    with col1:
        st.metric(
            METRICS['throughput']['name'],
            f"{int(throughput):,}",
            help="Messages processed per second by Kafka producer"
        )

    # Latency metric with delta
    with col2:
        delta_value = None
        if iteration > 10:
            delta_value = f"{int(latency - MONITORING_CONFIG['normal_latency_mean'])} ms"
        st.metric(
            METRICS['latency']['name'],
            f"{int(latency)}",
            delta=delta_value,
            delta_color="inverse",
            help="Processing latency in Spark streaming"
        )

    # Status indicator
    with col3:
        status_class = "status-healthy"
        if "WARNING" in status:
            status_class = "status-warning"
        elif "BACKPRESSURE" in status:
            status_class = "status-critical"

        st.markdown(f'<div class="metric-container"><h3 class="{status_class}">{status}</h3></div>',
                   unsafe_allow_html=True)

def create_live_chart(data: pd.DataFrame):
    """Create and display the live telemetry chart."""
    if data.empty:
        st.info("Waiting for data...")
        return

    try:
        fig = go.Figure()

        # Throughput line
        fig.add_trace(go.Scatter(
            x=data['Time'],
            y=data['Throughput'],
            name=METRICS['throughput']['name'],
            line=dict(color=METRICS['throughput']['color'], width=2),
            mode='lines+markers'
        ))

        # Latency line (secondary y-axis)
        fig.add_trace(go.Scatter(
            x=data['Time'],
            y=data['Latency'],
            name=METRICS['latency']['name'],
            line=dict(color=METRICS['latency']['color'], width=2, dash='dot'),
            yaxis="y2",
            mode='lines+markers'
        ))

        # Update layout
        fig.update_layout(
            title={
                'text': "📊 Live Pipeline Telemetry",
                'y':0.95,
                'x':0.5,
                'xanchor': 'center',
                'yanchor': 'top'
            },
            yaxis=dict(
                title=f"{METRICS['throughput']['name']}",
                titlefont=dict(color=METRICS['throughput']['color']),
                tickfont=dict(color=METRICS['throughput']['color'])
            ),
            yaxis2=dict(
                title=f"{METRICS['latency']['name']}",
                titlefont=dict(color=METRICS['latency']['color']),
                tickfont=dict(color=METRICS['latency']['color']),
                overlaying="y",
                side="right"
            ),
            margin=dict(l=0, r=0, t=50, b=0),
            height=400,
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )

        # Add range slider
        fig.update_xaxes(rangeslider_visible=True)

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        logger.error(f"Error creating chart: {e}")
        st.error("Error displaying chart. Please check the logs.")

def main():
    """Main application entry point."""
    try:
        logger.info("Starting Pipeline Monitor Dashboard")

        setup_page_config()
        initialize_session_state()

        # Header
        st.title("🌊 Enterprise Data Pipeline Monitor")
        st.markdown("""
        **Real-time monitoring dashboard for streaming data pipelines**

        Simulating: **Kafka Producer** → **Apache Spark Streaming** → **Data Warehouse**
        """)

        # Sidebar with information
        with st.sidebar:
            st.header("ℹ️ Pipeline Info")
            st.markdown("""
            **Current Status:**
            - Kafka Topic: `user_activity`
            - Spark Batch Duration: 2s
            - Monitoring: Live telemetry

            **Anomaly Detection:**
            - Backpressure alerts
            - Latency monitoring
            - Throughput analysis
            """)

            if st.button("🔄 Reset Dashboard"):
                st.session_state.data = pd.DataFrame(columns=['Time', 'Throughput', 'Latency', 'Status'])
                st.session_state.iteration = 0
                st.rerun()

        # Main dashboard
        placeholder = st.empty()

        while True:
            with placeholder.container():
                # Generate new metrics
                throughput, latency, status = generate_metrics(st.session_state.iteration)

                # Update data
                current_time = datetime.now()
                new_row = pd.DataFrame({
                    'Time': [current_time],
                    'Throughput': [throughput],
                    'Latency': [latency],
                    'Status': [status]
                })

                st.session_state.data = pd.concat([st.session_state.data, new_row]).tail(DASHBOARD_CONFIG['max_history_points'])

                # Display metrics
                create_metrics_display(throughput, latency, status, st.session_state.iteration)

                # Display chart
                create_live_chart(st.session_state.data)

                # Footer
                st.markdown("---")
                st.caption(f"Last updated: {current_time.strftime('%H:%M:%S')} | Iteration: {st.session_state.iteration}")

                st.session_state.iteration += 1

            time.sleep(DASHBOARD_CONFIG['update_interval'])

    except KeyboardInterrupt:
        logger.info("Dashboard stopped by user")
        st.stop()
    except Exception as e:
        logger.error(f"Critical error in main loop: {e}")
        st.error(f"Application error: {e}")
        st.stop()

if __name__ == "__main__":
    main()
    )
    
    chart_placeholder.plotly_chart(fig, use_container_width=True)
    time.sleep(0.5)
