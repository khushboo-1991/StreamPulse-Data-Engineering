# ============================================================
# connection.py
# StreamPulse - Azure Event Hubs Connection
# Sends streaming watch events to Event Hubs
# Author: Khushboo Patel
# ============================================================

import json
import logging
from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# ── Logging Setup ─────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Event Hubs Configuration ──────────────────────────────────
CONNECTION_STRING = os.getenv("EVENTHUB_CONNECTION_STRING")
EVENTHUB_NAME     = os.getenv("EVENTHUB_NAME")


def send_to_event_hub(event: dict) -> dict:
    """
    Sends a single streaming watch event to Azure Event Hubs.

    Args:
        event: dictionary containing watch event details

    Returns:
        dict with success status and event_id
    """
    try:
        # Create producer client
        producer = EventHubProducerClient.from_connection_string(
            conn_str=CONNECTION_STRING,
            eventhub_name=EVENTHUB_NAME
        )

        with producer:
            # Create a batch
            event_data_batch = producer.create_batch()

            # Convert event to JSON and add to batch
            event_data_batch.add(
                EventData(json.dumps(event))
            )

            # Send the batch
            producer.send_batch(event_data_batch)

            logger.info(
                f"✅ Event sent successfully | "
                f"event_id: {event['event_id']} | "
                f"content: {event['content_title']} | "
                f"user: {event['user_id']}"
            )

            return {
                "status": "success",
                "event_id": event["event_id"],
                "content": event["content_title"],
                "user": event["user_id"]
            }

    except Exception as e:
        logger.error(f"❌ Failed to send event: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }


def send_bulk_to_event_hub(events: list) -> dict:
    """
    Sends multiple streaming events to Azure Event Hubs.
    Used for testing and bulk data simulation.

    Args:
        events: list of event dictionaries

    Returns:
        dict with success count and failed count
    """
    success_count = 0
    failed_count  = 0

    try:
        producer = EventHubProducerClient.from_connection_string(
            conn_str=CONNECTION_STRING,
            eventhub_name=EVENTHUB_NAME
        )

        with producer:
            event_data_batch = producer.create_batch()

            for event in events:
                try:
                    event_data_batch.add(
                        EventData(json.dumps(event))
                    )
                    success_count += 1
                except Exception as e:
                    logger.error(f"❌ Failed to add event: {str(e)}")
                    failed_count += 1

            # Send all events in one batch
            producer.send_batch(event_data_batch)

            logger.info(
                f"✅ Bulk send complete | "
                f"Success: {success_count} | "
                f"Failed: {failed_count}"
            )

    except Exception as e:
        logger.error(f"❌ Bulk send failed: {str(e)}")

    return {
        "status": "complete",
        "success_count": success_count,
        "failed_count": failed_count
    }