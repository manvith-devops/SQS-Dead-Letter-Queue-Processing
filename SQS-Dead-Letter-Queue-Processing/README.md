````md
# SQS Dead Letter Queue Processing

A serverless AWS messaging system built using Amazon SQS, AWS Lambda, SNS, and CloudWatch to demonstrate retry mechanisms, Dead Letter Queue (DLQ) handling, failure monitoring, and automated replay workflows.

---

## Overview

This project simulates real-world asynchronous message processing where failed messages are isolated into Dead Letter Queues after exceeding retry limits.

The implementation includes:
- Standard and FIFO SQS queues
- Lambda-based consumers
- DLQ monitoring
- SNS notifications
- Replay functionality for failed events
- Infrastructure deployment using AWS CDK

---

## Components

| Component | Purpose |
|---|---|
| `main-queue` | Main queue for standard message processing |
| `failed-messages` | Dead Letter Queue for failed standard messages |
| `main-queue-fifo.fifo` | FIFO queue with ordered processing |
| `failed-messages-fifo.fifo` | DLQ for FIFO queue failures |
| `ConsumerLambda` | Handles message processing from SQS |
| `DLQMonitorLambda` | Tracks and analyzes failed messages |
| `ReplayLambda` | Replays DLQ messages back into the queue |
| `DLQAlertTopic` | SNS topic for operational alerts |

---

## Processing Flow

### Message Consumption

The consumer Lambda polls messages from SQS in batches.

Key behaviors:
- Batch size up to 10 messages
- Independent processing per record
- Simulated processing failures
- Partial batch failure response enabled

Only failed messages are retried while successful records are acknowledged immediately.

---

### Retry Logic

If processing fails repeatedly, Amazon SQS automatically moves the message into the configured Dead Letter Queue after 3 failed attempts.

```text
Producer → Main Queue → Consumer Lambda → DLQ
````

This protects the system from repeatedly processing invalid or poison messages.

---

### DLQ Monitoring

The DLQ monitor Lambda is triggered whenever new records arrive in the Dead Letter Queue.

Responsibilities:

* Inspect failed messages
* Log failure metadata
* Detect recurring issues
* Publish SNS alerts

---

### Replay Workflow

The replay Lambda supports operational recovery by moving failed messages from the DLQ back into the primary queue.

Replay sequence:

```text
Dead Letter Queue → Replay Lambda → Main Queue
```

This allows failed workloads to be reprocessed without manual intervention.

---

## Deployment Steps

Install required dependencies:

```bash
pip install -r requirements.txt
```

Generate the CloudFormation template:

```bash
cdk synth
```

Deploy infrastructure to AWS:

```bash
cdk deploy
```

---

## Testing the System

Run the message publisher script:

```bash
python send_messages.py
```

The script automatically:

* Resolves queue URLs
* Sends messages to standard queues
* Sends messages to FIFO queues

---

## Monitoring & Logging

### CloudWatch Logs

Each Lambda writes logs to CloudWatch for:

* Processing activity
* Failure tracking
* Replay operations
* DLQ analysis

---

### SNS Notifications

SNS alerts are triggered when failed messages are detected inside the Dead Letter Queue.

You can subscribe using email:

```bash
aws sns subscribe \
  --topic-arn <TOPIC_ARN> \
  --protocol email \
  --notification-endpoint you@example.com
```

---

## Replay Failed Messages

Invoke the replay function manually:

```bash
aws lambda invoke \
  --function-name <ReplayLambdaName> \
  --payload '{}' \
  response.json
```

Check replay results:

```bash
cat response.json
```

---

## Features

* SQS retry handling
* DLQ integration
* Partial batch response processing
* FIFO queue implementation
* Content-based deduplication
* SNS alert notifications
* DLQ replay automation
* CloudWatch monitoring
* Infrastructure as Code with AWS CDK

---

## Technologies Used

* AWS Lambda
* Amazon SQS
* Amazon SNS
* Amazon CloudWatch
* AWS CDK
* Python
* IAM

---

## Future Enhancements

Potential improvements:

* CloudWatch dashboards
* AWS X-Ray tracing
* CI/CD integration using GitHub Actions
* Structured JSON logging
* Automated replay scheduling
* Metrics-based scaling
* Terraform implementation

---

## Validation Checklist

* [x] Messages retry automatically on failure
* [x] Failed messages move to DLQ after retry exhaustion
* [x] DLQ monitoring triggers successfully
* [x] SNS alerts are published
* [x] Replay Lambda restores failed messages
* [x] FIFO queue processing works correctly

```
```
