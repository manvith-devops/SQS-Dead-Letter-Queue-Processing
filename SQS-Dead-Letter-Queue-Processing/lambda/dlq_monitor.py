import json
import boto3
import logging
import os
from collections import defaultdict

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sns_client = boto3.client('sns')


def lambda_handler(event, context):
    failure_patterns = defaultdict(int)
    message_details = []

    for record in event['Records']:
        message_id = record['messageId']
        attributes = record.get('attributes', {})
        receive_count = int(attributes.get('ApproximateReceiveCount', 1))
        sent_timestamp = attributes.get('SentTimestamp', 'unknown')

        try:
            body = json.loads(record['body'])
        except (json.JSONDecodeError, TypeError):
            body = record['body']

        message_details.append({
            'messageId': message_id,
            'receiveCount': receive_count,
            'sentTimestamp': sent_timestamp,
            'body': body,
        })

        if receive_count >= 3:
            failure_patterns['exhausted_retries'] += 1
        else:
            failure_patterns['partial_retries'] += 1

        logger.info(
            f"DLQ message: id={message_id}, attempts={receive_count}, "
            f"sentAt={sent_timestamp}, body={body}"
        )

    total_failed = len(message_details)
    logger.warning(
        f"DLQ batch analysis: total={total_failed}, "
        f"exhausted_retries={failure_patterns['exhausted_retries']}, "
        f"partial_retries={failure_patterns['partial_retries']}"
    )

    sns_topic_arn = os.environ['SNS_TOPIC_ARN']
    failed_ids = [m['messageId'] for m in message_details]
    alert_body = (
        f"DLQ ALERT: {total_failed} message(s) failed processing.\n\n"
        f"Exhausted all 3 retries: {failure_patterns['exhausted_retries']}\n"
        f"Partial retries only: {failure_patterns['partial_retries']}\n\n"
        f"Failed message IDs:\n" + "\n".join(f"  - {mid}" for mid in failed_ids)
    )

    sns_client.publish(
        TopicArn=sns_topic_arn,
        Subject=f"DLQ Alert: {total_failed} message(s) failed",
        Message=alert_body,
    )
    logger.info("SNS alert published successfully")

    return {
        'statusCode': 200,
        'body': json.dumps({
            'processed': total_failed,
            'failure_patterns': dict(failure_patterns),
        }),
    }
