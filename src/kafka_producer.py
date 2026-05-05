"""
Kafka Producer Simulation Module

This module simulates a Kafka producer that generates realistic streaming
event data for user activity monitoring. It demonstrates how real-world
Kafka producers work in data streaming pipelines.

Author: Shiwanshu Inamdar
Date: 2024
"""

import json
import time
import random
import logging
from datetime import datetime
from typing import Dict, Any, List
from config import KAFKA_CONFIG, SIMULATION_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class KafkaProducerSimulator:
    """
    Simulates a Kafka producer for streaming user activity events.

    This class generates realistic event data that would typically come from
    web applications, mobile apps, or IoT devices in a real streaming pipeline.
    """

    def __init__(self, topic: str = None, brokers: List[str] = None):
        """
        Initialize the Kafka producer simulator.

        Args:
            topic: Kafka topic name (default from config)
            brokers: List of Kafka broker addresses (default from config)
        """
        self.topic = topic or KAFKA_CONFIG['topic']
        self.brokers = brokers or KAFKA_CONFIG['brokers']
        self.event_types = KAFKA_CONFIG['event_types']
        self.user_count = KAFKA_CONFIG['user_count']
        self.interval = KAFKA_CONFIG['event_generation_interval']

        logger.info(f"Initialized Kafka Producer for topic: {self.topic}")
        logger.info(f"Connected to brokers: {', '.join(self.brokers)}")

    def generate_event(self) -> Dict[str, Any]:
        """
        Generate a single user activity event.

        Returns:
            Dict containing event data with the following structure:
            {
                "event_id": str,
                "timestamp": str (ISO format),
                "event_type": str,
                "user_id": str,
                "payload_bytes": int
            }
        """
        try:
            event = {
                "event_id": f"evt_{random.randint(10000, 99999)}",
                "timestamp": datetime.now().isoformat(),
                "event_type": random.choice(self.event_types),
                "user_id": f"user_{random.randint(1, self.user_count)}",
                "payload_bytes": random.randint(
                    SIMULATION_CONFIG['payload_size_range'][0],
                    SIMULATION_CONFIG['payload_size_range'][1]
                )
            }

            # Add event-specific payload data
            if event['event_type'] == 'page_view':
                event['page_url'] = f"/page/{random.randint(1, 100)}"
                event['session_duration'] = random.randint(10, 300)
            elif event['event_type'] == 'add_to_cart':
                event['product_id'] = f"prod_{random.randint(1000, 9999)}"
                event['quantity'] = random.randint(1, 5)
            elif event['event_type'] == 'purchase':
                event['order_id'] = f"order_{random.randint(10000, 99999)}"
                event['total_amount'] = round(random.uniform(10.0, 500.0), 2)
            elif event['event_type'] == 'login':
                event['login_method'] = random.choice(['email', 'social', 'sso'])
                event['device_type'] = random.choice(['mobile', 'desktop', 'tablet'])

            return event

        except Exception as e:
            logger.error(f"Error generating event: {e}")
            return self._generate_fallback_event()

    def _generate_fallback_event(self) -> Dict[str, Any]:
        """Generate a minimal fallback event in case of errors."""
        return {
            "event_id": f"evt_{random.randint(10000, 99999)}",
            "timestamp": datetime.now().isoformat(),
            "event_type": "unknown",
            "user_id": f"user_{random.randint(1, self.user_count)}",
            "payload_bytes": 100
        }

    def send_event(self, event: Dict[str, Any]) -> bool:
        """
        Simulate sending an event to Kafka.

        In a real implementation, this would use the kafka-python library
        to send messages to actual Kafka brokers.

        Args:
            event: Event data to send

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Simulate network latency
            time.sleep(SIMULATION_CONFIG['event_delay_simulation'])

            # Log the event (simulating Kafka production)
            logger.info(f"[Topic: {self.topic}] Producing record: {json.dumps(event, indent=2)}")

            # In real Kafka, this would be:
            # producer.send(self.topic, value=event).get(timeout=10)

            return True

        except Exception as e:
            logger.error(f"Error sending event to Kafka: {e}")
            return False

    def simulate_stream(self, num_events: int = None) -> None:
        """
        Simulate a continuous stream of events to Kafka.

        Args:
            num_events: Number of events to generate (default from config)
        """
        num_events = num_events or SIMULATION_CONFIG['total_events_to_simulate']

        logger.info(f"Starting Kafka Producer Simulation with {num_events} events...")
        logger.info(f"Event generation interval: {self.interval} seconds")

        successful_events = 0
        failed_events = 0

        try:
            for i in range(num_events):
                event = self.generate_event()

                if self.send_event(event):
                    successful_events += 1
                else:
                    failed_events += 1

                # Progress logging
                if (i + 1) % 10 == 0:
                    logger.info(f"Processed {i + 1}/{num_events} events "
                              f"(Success: {successful_events}, Failed: {failed_events})")

                # Simulate streaming delay
                time.sleep(self.interval)

        except KeyboardInterrupt:
            logger.info("Kafka producer simulation stopped by user")
        except Exception as e:
            logger.error(f"Critical error in producer simulation: {e}")
        finally:
            logger.info(f"Producer simulation completed. "
                       f"Total: {successful_events + failed_events}, "
                       f"Success: {successful_events}, Failed: {failed_events}")

def main():
    """Main entry point for the Kafka producer simulation."""
    try:
        producer = KafkaProducerSimulator()
        producer.simulate_stream()
    except Exception as e:
        logger.error(f"Failed to start producer simulation: {e}")
        exit(1)

if __name__ == "__main__":
    main()
