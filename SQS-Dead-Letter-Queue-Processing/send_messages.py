import boto3
import json

sqs_client = boto3.client('sqs')

# Resolve queue URLs dynamically so no hardcoding is needed
main_queue_url = sqs_client.get_queue_url(QueueName='main-queue')['QueueUrl']
fifo_queue_url = sqs_client.get_queue_url(QueueName='main-queue-fifo.fifo')['QueueUrl']

print(f"Main queue : {main_queue_url}")
print(f"FIFO queue : {fifo_queue_url}")
print()

# --- Standard queue: 100 messages ---
print("Sending 100 messages to main-queue ...")
for i in range(100):
    message = {'id': i, 'data': f'Message {i}'}
    sqs_client.send_message(
        QueueUrl=main_queue_url,
        MessageBody=json.dumps(message),
    )
    print(f"  [standard] sent message {i}")

print("Done — 100 standard messages sent.\n")

# --- FIFO queue: 100 messages with deduplication ---
print("Sending 100 messages to main-queue-fifo.fifo ...")
for i in range(100):
    message = {'id': i, 'data': f'FIFO Message {i}'}
    sqs_client.send_message(
        QueueUrl=fifo_queue_url,
        MessageBody=json.dumps(message),
        MessageGroupId='group1',
        MessageDeduplicationId=str(i),  # content-based dedup also enabled on queue
    )
    print(f"  [fifo] sent message {i}")

print("Done — 100 FIFO messages sent.")
