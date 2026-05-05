"""
Apache Spark Streaming Processor Simulation Module

This module simulates Apache Spark Structured Streaming processing of
micro-batches from Kafka topics. It demonstrates real-world streaming
data processing patterns including aggregations, filtering, and
data warehouse writes.

Author: Shiwanshu Inamdar
Date: 2024
"""

import pandas as pd
import time
import logging
import random
from datetime import datetime
from typing import Dict, Any, List, Optional
from config import SPARK_CONFIG, DW_CONFIG, KAFKA_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SparkStreamingSimulator:
    """
    Simulates Apache Spark Structured Streaming for processing Kafka data.

    This class demonstrates how Spark processes micro-batches of streaming
    data, performs aggregations, and writes results to a data warehouse.
    """

    def __init__(self):
        """Initialize the Spark streaming simulator."""
        self.app_name = SPARK_CONFIG['app_name']
        self.batch_duration = SPARK_CONFIG['batch_duration']
        self.checkpoint_dir = SPARK_CONFIG['checkpoint_dir']
        self.topic = KAFKA_CONFIG['topic']

        logger.info(f"Initialized Spark Streaming Context: {self.app_name}")
        logger.info(f"Batch duration: {self.batch_duration} seconds")
        logger.info(f"Checkpoint directory: {self.checkpoint_dir}")

    def simulate_kafka_read(self, batch_size: int = 100) -> pd.DataFrame:
        """
        Simulate reading a micro-batch from Kafka topic.

        In a real Spark application, this would be:
        df = spark.readStream.format("kafka")...

        Args:
            batch_size: Number of events in this micro-batch

        Returns:
            DataFrame with simulated Kafka events
        """
        try:
            # Generate sample events for this batch
            events = []
            event_types = KAFKA_CONFIG['event_types']

            for _ in range(batch_size):
                event = {
                    'event_id': f'evt_{random.randint(10000, 99999)}',
                    'timestamp': datetime.now(),
                    'event_type': random.choice(event_types),
                    'user_id': f'user_{random.randint(1, KAFKA_CONFIG["user_count"])}',
                    'payload_bytes': random.randint(100, 5000),
                    'processing_timestamp': datetime.now()
                }
                events.append(event)

            df = pd.DataFrame(events)
            logger.info(f"Simulated reading {len(df)} events from Kafka topic '{self.topic}'")
            return df

        except Exception as e:
            logger.error(f"Error simulating Kafka read: {e}")
            return pd.DataFrame()

    def process_microbatch(self, batch_df: pd.DataFrame, batch_id: int) -> bool:
        """
        Process a single micro-batch of streaming data.

        This simulates the foreachBatch operation in Spark Structured Streaming,
        including data transformations, aggregations, and warehouse writes.

        Args:
            batch_df: DataFrame containing the micro-batch data
            batch_id: Unique identifier for this batch

        Returns:
            bool: True if processing successful, False otherwise
        """
        try:
            logger.info(f"{'='*50}")
            logger.info(f"Processing Micro-Batch {batch_id}")
            logger.info(f"{'='*50}")

            if batch_df.empty:
                logger.warning("Empty batch received - skipping processing")
                return True

            # Log batch statistics
            logger.info(f"Batch size: {len(batch_df)} events")
            logger.info(f"Time range: {batch_df['timestamp'].min()} to {batch_df['timestamp'].max()}")

            # 1. Data Quality Checks
            self._perform_data_quality_checks(batch_df)

            # 2. Filter and Transform Data
            processed_df = self._filter_and_transform(batch_df)

            # 3. Perform Aggregations
            aggregations = self._perform_aggregations(processed_df)

            # 4. Write to Data Warehouse
            success = self._write_to_data_warehouse(aggregations, batch_id)

            if success:
                logger.info(f"✅ Successfully committed batch {batch_id} ({len(processed_df)} records)")
            else:
                logger.error(f"❌ Failed to commit batch {batch_id}")

            return success

        except Exception as e:
            logger.error(f"Error processing micro-batch {batch_id}: {e}")
            return False

    def _perform_data_quality_checks(self, df: pd.DataFrame) -> None:
        """Perform basic data quality checks on the batch."""
        try:
            # Check for null values
            null_counts = df.isnull().sum()
            if null_counts.any():
                logger.warning(f"Null values found: {null_counts[null_counts > 0].to_dict()}")

            # Check for duplicate event_ids
            duplicates = df['event_id'].duplicated().sum()
            if duplicates > 0:
                logger.warning(f"Found {duplicates} duplicate event_ids")

            # Check timestamp validity
            invalid_timestamps = pd.to_datetime(df['timestamp'], errors='coerce').isnull().sum()
            if invalid_timestamps > 0:
                logger.warning(f"Found {invalid_timestamps} invalid timestamps")

            logger.info("Data quality checks completed")

        except Exception as e:
            logger.error(f"Error in data quality checks: {e}")

    def _filter_and_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter and transform the raw batch data.

        In real Spark, this would involve:
        - Filtering out invalid records
        - Adding computed columns
        - Data type conversions
        """
        try:
            # Filter out events with invalid data
            filtered_df = df.dropna(subset=['event_id', 'timestamp', 'event_type'])

            # Add processing metadata
            filtered_df = filtered_df.copy()
            filtered_df['batch_processing_time'] = datetime.now()
            filtered_df['data_quality_score'] = filtered_df.apply(
                lambda row: self._calculate_data_quality_score(row), axis=1
            )

            logger.info(f"Filtered {len(df) - len(filtered_df)} invalid records")
            logger.info(f"Remaining valid records: {len(filtered_df)}")

            return filtered_df

        except Exception as e:
            logger.error(f"Error in filtering/transformation: {e}")
            return df

    def _calculate_data_quality_score(self, row) -> float:
        """Calculate a simple data quality score for a record."""
        score = 1.0

        # Deduct points for missing optional fields
        if pd.isna(row.get('user_id')):
            score -= 0.2
        if pd.isna(row.get('payload_bytes')):
            score -= 0.1

        return max(0.0, score)

    def _perform_aggregations(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Perform aggregations on the processed batch data.

        Returns:
            Dictionary containing various aggregation results
        """
        try:
            aggregations = {}

            # Event type distribution
            event_counts = df.groupby('event_type').size().reset_index(name='count')
            aggregations['event_distribution'] = event_counts.to_dict('records')

            # User activity summary
            user_activity = df.groupby('user_id').size().reset_index(name='event_count')
            aggregations['user_activity'] = {
                'total_unique_users': len(user_activity),
                'avg_events_per_user': user_activity['event_count'].mean(),
                'max_events_per_user': user_activity['event_count'].max()
            }

            # Payload statistics
            aggregations['payload_stats'] = {
                'total_bytes': df['payload_bytes'].sum(),
                'avg_payload_size': df['payload_bytes'].mean(),
                'max_payload_size': df['payload_bytes'].max()
            }

            # Time-based aggregations
            df['hour'] = df['timestamp'].dt.hour
            hourly_activity = df.groupby('hour').size().reset_index(name='count')
            aggregations['hourly_activity'] = hourly_activity.to_dict('records')

            logger.info("Batch Aggregation Results:")
            logger.info(f"- Event Distribution: {len(event_counts)} event types")
            logger.info(f"- Unique Users: {aggregations['user_activity']['total_unique_users']}")
            logger.info(f"- Total Payload: {aggregations['payload_stats']['total_bytes']:,} bytes")

            return aggregations

        except Exception as e:
            logger.error(f"Error performing aggregations: {e}")
            return {}

    def _write_to_data_warehouse(self, aggregations: Dict[str, Any], batch_id: int) -> bool:
        """
        Simulate writing aggregated results to a data warehouse.

        In production, this would write to Delta Lake, Snowflake, Redshift, etc.
        """
        try:
            if DW_CONFIG['connection_type'] == 'simulated':
                # Simulate warehouse write with realistic delay
                time.sleep(0.1)

                logger.info(f"Simulated writing batch {batch_id} to Data Warehouse")
                logger.info(f"Connection: {DW_CONFIG['connection_type']}")
                logger.info(f"Mode: {DW_CONFIG['batch_mode']}")

                # Log some sample results
                if 'event_distribution' in aggregations:
                    for event in aggregations['event_distribution'][:3]:  # Show first 3
                        logger.info(f"  {event['event_type']}: {event['count']} events")

                return True

            else:
                # Placeholder for real warehouse connections
                logger.warning(f"Real {DW_CONFIG['connection_type']} connection not implemented")
                return False

        except Exception as e:
            logger.error(f"Error writing to data warehouse: {e}")
            return False

    def start_streaming_context(self) -> None:
        """
        Start the simulated streaming context.

        This simulates Spark's awaitTermination() loop where micro-batches
        are continuously processed.
        """
        logger.info("Starting Apache Spark Structured Streaming Context...")
        logger.info(f"Awaiting data from Kafka topic '{self.topic}'...")
        logger.info("Streaming query started successfully")
        logger.info(f"Batch interval: {self.batch_duration} seconds")

        batch_id = 1

        try:
            while True:
                # Simulate waiting for next micro-batch
                time.sleep(self.batch_duration)

                # Generate and process a micro-batch
                batch_df = self.simulate_kafka_read(random.randint(50, 150))
                success = self.process_microbatch(batch_df, batch_id)

                if not success:
                    logger.warning(f"Batch {batch_id} processing failed - continuing...")

                batch_id += 1

        except KeyboardInterrupt:
            logger.info("Streaming context terminated by user")
        except Exception as e:
            logger.error(f"Critical error in streaming context: {e}")
        finally:
            logger.info("Spark Streaming Context terminated")

def main():
    """Main entry point for the Spark streaming simulation."""
    try:
        simulator = SparkStreamingSimulator()
        simulator.start_streaming_context()
    except Exception as e:
        logger.error(f"Failed to start Spark streaming simulation: {e}")
        exit(1)

if __name__ == "__main__":
    main()
