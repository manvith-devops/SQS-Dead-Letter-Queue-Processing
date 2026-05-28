import boto3

sqs  = boto3.client('sqs',  region_name='us-east-1')
logs = boto3.client('logs', region_name='us-east-1')

QUEUES = [
    ('https://sqs.us-east-1.amazonaws.com/866934333672/main-queue',     'main-queue'),
    ('https://sqs.us-east-1.amazonaws.com/866934333672/failed-messages', 'failed-messages DLQ'),
]

print('=== Queue depths ===')
for url, name in QUEUES:
    r = sqs.get_queue_attributes(
        QueueUrl=url,
        AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'],
    )
    a = r['Attributes']
    visible   = a['ApproximateNumberOfMessages']
    in_flight = a['ApproximateNumberOfMessagesNotVisible']
    print(f'  {name:30s}: visible={visible}  in-flight={in_flight}')

LOG_GROUPS = [
    ('/aws/lambda/Assignment17Stack-ReplayLambdaAFD97013-N3wpWzRFeu55',    'ReplayLambda'),
    ('/aws/lambda/Assignment17Stack-ConsumerLambdaF347BC65-n6Ul2rci9wxu',  'ConsumerLambda (post-replay)'),
]

for lg, label in LOG_GROUPS:
    print(f'\n=== {label} ===')
    try:
        streams = logs.describe_log_streams(
            logGroupName=lg, orderBy='LastEventTime', descending=True, limit=1
        )
        for s in streams['logStreams']:
            events = logs.get_log_events(
                logGroupName=lg,
                logStreamName=s['logStreamName'],
                limit=25,
                startFromHead=False,
            )
            for e in events['events']:
                msg = e['message'].strip()
                if msg:
                    print(msg)
    except Exception as ex:
        print(f'  Error: {ex}')
