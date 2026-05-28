import json
import os
import random
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Default 20% failure. Override via FAILURE_RATE env var (0.0–1.0).
FAILURE_RATE = float(os.environ.get("FAILURE_RATE", "0.2"))


def lambda_handler(event, context):
    batch_item_failures = []

    for record in event['Records']:
        message_id = record['messageId']
        receive_count = int(record.get('attributes', {}).get('ApproximateReceiveCount', 1))

        try:
            message = json.loads(record['body'])
            logger.info(
                f"Processing message: id={message_id}, attempt={receive_count}, body={message}"
            )

            if random.random() < FAILURE_RATE:
                raise Exception(
                    f"Simulated {int(FAILURE_RATE*100)}%% failure for message id={message_id}"
                )

            logger.info(f"Successfully processed message id={message_id}")

        except Exception as e:
            logger.error(
                f"Failed message id={message_id} on attempt {receive_count}: {e}"
            )
            batch_item_failures.append({"itemIdentifier": message_id})

    logger.info(
        f"Batch complete: {len(event['Records'])} total, "
        f"{len(batch_item_failures)} failed"
    )
    return {"batchItemFailures": batch_item_failures}
