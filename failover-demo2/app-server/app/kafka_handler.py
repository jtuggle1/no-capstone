from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from typing import Dict, Any
import json
import logging
import asyncio
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

class KafkaHandler:
    def __init__(self, settings):
        self.settings = settings
        self.producer = None
        self.consumer = None
        self.is_running = False
        self._consumer_task = None
        self.mongo_handler = None  

    # Add setter for MongoDB handler
    def set_mongo_handler(self, mongo_handler):
        """Set the MongoDB handler for persisting transactions."""
        self.mongo_handler = mongo_handler

    async def start(self):
        """Start the Kafka producer and consumer."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.settings.KAFKA_BOOTSTRAP_SERVERS.split(','),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            
            self.consumer = KafkaConsumer(
                self.settings.KAFKA_TOPIC,
                bootstrap_servers=self.settings.KAFKA_BOOTSTRAP_SERVERS.split(','),
                group_id=self.settings.KAFKA_CONSUMER_GROUP,
                auto_offset_reset=self.settings.KAFKA_AUTO_OFFSET_RESET,
                enable_auto_commit=self.settings.KAFKA_ENABLE_AUTO_COMMIT,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            
            self.is_running = True
            self._consumer_task = asyncio.create_task(self._consume_messages())
            logger.info("Kafka handler started successfully")
            
        except KafkaError as e:
            logger.error(f"Failed to start Kafka handler: {str(e)}")
            raise

    async def stop(self):
        """Stop the Kafka producer and consumer."""
        self.is_running = False
        if self._consumer_task:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
        
        if self.producer:
            self.producer.close()
        if self.consumer:
            self.consumer.close()
        
        logger.info("Kafka handler stopped")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def send_message(self, topic: str, message: Dict[str, Any]):
        """Send message to Kafka topic."""
        try:
            future = self.producer.send(topic, message)
            result = await asyncio.to_thread(future.get, timeout=10)
            logger.info(f"Message sent to topic {topic}: {message['transaction_id']}")
            return result
        except Exception as e:
            logger.error(f"Failed to send message to Kafka: {str(e)}")
            raise

    async def _consume_messages(self):
        """Consume messages from Kafka topic."""
        while self.is_running:
            try:
                message_batch = await asyncio.to_thread(
                    self.consumer.poll, timeout_ms=1000
                )
                
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        await self._process_message(message)
                        
            except Exception as e:
                logger.error(f"Error consuming messages: {str(e)}")
                await asyncio.sleep(1)

    async def _process_message(self, message):
        """Process consumed message and persist to MongoDB."""
        try:
            transaction_data = message.value
            logger.info(f"Processing transaction: {transaction_data['transaction_id']}")
            
            # Update transaction status to processing
            transaction_data['status'] = 'processing'
            
            # Save to MongoDB
            if self.mongo_handler:
                saved = await self.mongo_handler.save_transaction(transaction_data)
                if saved:
                    # Update status to completed
                    await self.mongo_handler.update_transaction_status(
                        transaction_data['transaction_id'],
                        'completed'
                    )
                    logger.info(f"Transaction {transaction_data['transaction_id']} processed and saved")
                else:
                    logger.error(f"Failed to save transaction {transaction_data['transaction_id']}")
                    await self.mongo_handler.update_transaction_status(
                        transaction_data['transaction_id'],
                        'failed'
                    )
            else:
                logger.error("MongoDB handler not set - cannot persist transaction")
                
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            if self.mongo_handler:
                await self.mongo_handler.update_transaction_status(
                    transaction_data['transaction_id'],
                    'failed'
                )

    async def is_healthy(self) -> bool:
        """Check if Kafka connection is healthy."""
        try:
            if not self.producer or not self.consumer:
                return False
            
            # Try to send a health check message
            test_message = {
                "health_check": True,
                "timestamp": datetime.utcnow().isoformat()
            }
            await self.send_message("health_checks", test_message)
            return True
            
        except Exception as e:
            logger.error(f"Kafka health check failed: {str(e)}")
            return False