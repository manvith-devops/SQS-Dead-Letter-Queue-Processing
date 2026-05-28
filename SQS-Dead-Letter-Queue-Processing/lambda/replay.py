import json
import boto3
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sqs_client = boto3.client('sqs')


def lambda_handler(event, context):
    dlq_url = os.environ['DLQ_URL']
    main_queue_url = os.environ['MAIN_QUEUE_URL']

    total_replayed = 0
    total_failed = 0

    # Drain the DLQ in pages of 10
    while True:
        response = sqs_client.receive_message(
            QueueUrl=dlq_url,
            MaxNumberOfMessages=10,
            AttributeNames=['All'],
            WaitTimeSeconds=1,
        )

        messages = response.get('Messages', [])
        if not messages:
            logger.info("No more messages in DLQ — replay complete")
            break

        for msg in messages:
            msg_id = msg['MessageId']
            try:
                sqs_client.send_message(
                    QueueUrl=main_queue_url,
                    MessageBody=msg['Body'],
                )
                sqs_client.delete_message(
                    QueueUrl=dlq_url,
                    ReceiptHandle=msg['ReceiptHandle'],
                )
                logger.info(f"Replayed message id={msg_id}: {msg['Body']}")
                total_replayed += 1
            except Exception as e:
                logger.error(f"Failed to replay message id={msg_id}: {e}")
                total_failed += 1

    logger.info(
        f"Replay summary: replayed={total_replayed}, failed_to_replay={total_failed}"
    )
    return {
        'statusCode': 200,
        'body': json.dumps({'replayed': total_replayed, 'failed': total_failed}),
    }
