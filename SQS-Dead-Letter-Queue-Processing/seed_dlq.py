import boto3
import json

sqs = boto3.client('sqs', region_name='us-east-1')
dlq_url = 'https://sqs.us-east-1.amazonaws.com/866934333672/failed-messages'

for i in range(5):
    sqs.send_message(
        QueueUrl=dlq_url,
        MessageBody=json.dumps({'id': i, 'data': f'Replay-demo message {i}'}),
    )
    print(f'Seeded DLQ with replay-demo message {i}')

r = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=['ApproximateNumberOfMessages'])
print(f"DLQ depth after seeding: {r['Attributes']['ApproximateNumberOfMessages']}")
