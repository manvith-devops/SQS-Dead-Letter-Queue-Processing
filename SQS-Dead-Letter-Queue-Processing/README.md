
# SQS Dead Letter Queue Processing

This CDK project implements robust message processing with retry and Dead Letter Queue (DLQ)
handling using AWS SQS, Lambda, SNS, and CloudWatch.

## Architecture

| Resource | Name | Details |
|---|---|---|
| Standard queue | `main-queue` | Visibility timeout 30 s, redrive after 3 failures |
| Standard DLQ | `failed-messages` | 14-day retention |
| FIFO queue | `main-queue-fifo.fifo` | Content-based deduplication enabled |
| FIFO DLQ | `failed-messages-fifo.fifo` | 14-day retention |
| Consumer Lambda | `ConsumerLambda` | Processes messages, simulates 20 % failure, partial-batch response |
| DLQ Monitor Lambda | `DLQMonitorLambda` | Analyzes failure patterns, sends SNS alert |
| Replay Lambda | `ReplayLambda` | Drains entire DLQ back into the main queue |
| SNS Topic | `DLQAlertTopic` | Receives DLQ failure notifications |

## How It Works

1. **Consumer** receives batches of up to 10 messages from `main-queue`.  
   Each message is processed individually — 20 % are intentionally failed.  
   Only failed message IDs are returned (`batchItemFailures`), so successful ones are **not** retried.

2. After a message fails **3 times** SQS automatically moves it to `failed-messages` (DLQ).

3. **DLQ Monitor** is triggered when new messages appear in `failed-messages`.  
   It logs each failure, counts retry-exhaustion vs partial-failure patterns, then publishes an SNS alert.

4. **Replay Lambda** can be invoked manually to drain the entire DLQ back into `main-queue` for reprocessing.

## Deployment

```bash
pip install -r requirements.txt
cdk synth      # validate the template
cdk deploy     # deploy to AWS
```

> If you hit the IAM-role quota (1 000 roles per account) you will need to delete unused roles
> or request a quota increase before deploying.

## Sending Test Messages

`send_messages.py` resolves queue URLs automatically from your AWS configuration — no manual edits needed.

```bash
python send_messages.py
```

This sends 100 messages to `main-queue` and 100 to `main-queue-fifo.fifo`.

## Monitoring

- **CloudWatch Logs**: each Lambda writes to its own log group (`/aws/lambda/<function-name>`, 1-week retention).
- **SNS Alerts**: subscribe an email address to the `DLQAlertTopic` to receive DLQ failure notifications.

## Replaying DLQ Messages

Invoke the Replay Lambda from the AWS Console or CLI:

```bash
aws lambda invoke \
  --function-name <ReplayLambdaName from CDK outputs> \
  --payload '{}' \
  response.json
cat response.json
```

The Lambda pages through the entire DLQ (10 messages at a time), re-sends each to `main-queue`,
and deletes it from the DLQ.

## Success Criteria

- [x] Failed messages move to DLQ after 3 attempts
- [x] DLQ Monitor Lambda triggers on new DLQ messages and sends SNS alert
- [x] Replay Lambda drains DLQ back to main queue
- [x] Partial-batch response ensures successfully-processed messages are never retried
- [x] FIFO queue version with content-based deduplication
