#!/usr/bin/env python3
"""
kafka_producer.py

This module provides the KafkaProducerWrapper class using aiokafka for asynchronous message production.
It is designed for the Discovery Service in the AI-Powered Identity Risk Analytics Platform.
Messages are JSON-serialized and sent asynchronously to a specified Kafka topic.
Configuration (e.g., bootstrap servers) is passed as a dictionary.
The producer supports asynchronous sending, flushing, and graceful shutdown.

Author: [Your Name]
Date: [Current Date]
"""

import asyncio
import json
import logging
from aiokafka import AIOKafkaProducer

class KafkaProducerWrapper:
    def __init__(self, config: dict):
        """
        Initializes the KafkaProducerWrapper with the given configuration.

        Parameters:
            config (dict): A configuration dictionary containing:
                - "bootstrap_servers": List of Kafka bootstrap servers.
                - Optionally, additional Kafka settings.
        """
        self.config = config
        self.bootstrap_servers = config.get("bootstrap_servers", ["localhost:9092"])
        self.logger = logging.getLogger("KafkaProducerWrapper")
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        self.started = False

    async def start(self) -> None:
        """
        Starts the AIOKafkaProducer asynchronously.
        """
        if not self.started:
            await self.producer.start()
            self.started = True
            self.logger.info(f"AIOKafkaProducer started with bootstrap servers: {self.bootstrap_servers}")

    async def send(self, topic: str, value: dict, key: str = None) -> None:
        """
        Sends a message to a specified Kafka topic asynchronously.

        Parameters:
            topic (str): The Kafka topic to send the message to.
            value (dict): The message payload (will be JSON serialized).
            key (str): Optional key for the message (will be encoded to bytes if provided).
        """
        if not self.started:
            await self.start()
        try:
            key_bytes = key.encode("utf-8") if key else None
            # Send the message and wait for the send to complete.
            record_metadata = await self.producer.send_and_wait(topic, value=value, key=key_bytes)
            self.logger.debug(
                f"Message sent to topic '{record_metadata.topic}', partition {record_metadata.partition}, offset {record_metadata.offset}"
            )
        except Exception as e:
            self.logger.error(f"Error sending message to topic '{topic}': {e}")

    async def flush(self) -> None:
        """
        Flushes the Kafka producer to ensure all pending messages are sent.
        """
        if self.started:
            await self.producer.flush()
            self.logger.info("AIOKafkaProducer flushed successfully.")

    async def close(self) -> None:
        """
        Closes the Kafka producer gracefully.
        """
        if self.started:
            await self.producer.stop()
            self.started = False
            self.logger.info("AIOKafkaProducer closed successfully.")

# Test block: run only if this module is executed directly.
if __name__ == "__main__":
    async def main():
        # Set up basic logging configuration.
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        logger = logging.getLogger("KafkaProducerTest")

        # Example configuration for Kafka.
        test_config = {
            "bootstrap_servers": ["localhost:9092"],
            "identity_topic": "discovery-identity"
        }

        # Initialize the KafkaProducerWrapper.
        producer_wrapper = KafkaProducerWrapper(test_config)
        await producer_wrapper.start()

        # Example message to send.
        test_message = {"user_id": 1, "username": "test_user", "source": "ad"}

        # Send the test message to the identity topic.
        await producer_wrapper.send(test_config.get("identity_topic"), test_message)

        # Flush pending messages.
        await producer_wrapper.flush()

        # Close the producer gracefully.
        await producer_wrapper.close()

        logger.info("AIOKafkaProducer test completed.")

    asyncio.run(main())
