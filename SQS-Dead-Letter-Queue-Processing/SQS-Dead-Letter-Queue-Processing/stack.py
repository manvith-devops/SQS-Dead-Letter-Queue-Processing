from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_lambda_event_sources as lambda_events,
    aws_logs as logs,
    aws_sns as sns,
    aws_sqs as sqs,
)
from constructs import Construct


class Assignment17Stack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ── Dead-letter queues ────────────────────────────────────────────────
        dlq = sqs.Queue(
            self, "FailedMessagesDLQ",
            queue_name="failed-messages",
            retention_period=Duration.days(14),
            removal_policy=RemovalPolicy.DESTROY,
        )

        fifo_dlq = sqs.Queue(
            self, "FailedMessagesFIFODLQ",
            queue_name="failed-messages-fifo.fifo",
            fifo=True,
            retention_period=Duration.days(14),
            removal_policy=RemovalPolicy.DESTROY,
        )

        # ── Main queues ───────────────────────────────────────────────────────
        main_queue = sqs.Queue(
            self, "MainQueue",
            queue_name="main-queue",
            visibility_timeout=Duration.seconds(30),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=dlq,
            ),
            removal_policy=RemovalPolicy.DESTROY,
        )

        fifo_main_queue = sqs.Queue(
            self, "FIFOMainQueue",
            queue_name="main-queue-fifo.fifo",
            fifo=True,
            visibility_timeout=Duration.seconds(30),
            content_based_deduplication=True,
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=fifo_dlq,
            ),
            removal_policy=RemovalPolicy.DESTROY,
        )

        # ── SNS alert topic ───────────────────────────────────────────────────
        sns_topic = sns.Topic(
            self, "DLQAlertTopic",
            display_name="DLQ Alerts",
        )

        # ── Shared IAM role ───────────────────────────────────────────────────
        lambda_role = iam.Role(
            self, "LambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSQSFullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSNSFullAccess"),
            ],
        )

        # ── Consumer Lambda ───────────────────────────────────────────────────
        # report_batch_item_failures lets the consumer return only the
        # failed message IDs so successful messages are not retried.
        consumer_lambda = _lambda.Function(
            self, "ConsumerLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset("lambda"),
            handler="consumer.lambda_handler",
            timeout=Duration.seconds(30),
            role=lambda_role,
            log_retention=logs.RetentionDays.ONE_WEEK,
        )

        consumer_lambda.add_event_source(
            lambda_events.SqsEventSource(
                main_queue,
                batch_size=10,
                report_batch_item_failures=True,
            )
        )

        consumer_lambda.add_event_source(
            lambda_events.SqsEventSource(
                fifo_main_queue,
                batch_size=10,
                report_batch_item_failures=True,
            )
        )

        # ── DLQ Monitor Lambda ────────────────────────────────────────────────
        dlq_monitor_lambda = _lambda.Function(
            self, "DLQMonitorLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset("lambda"),
            handler="dlq_monitor.lambda_handler",
            timeout=Duration.seconds(30),
            role=lambda_role,
            log_retention=logs.RetentionDays.ONE_WEEK,
            environment={
                "SNS_TOPIC_ARN": sns_topic.topic_arn,
            },
        )

        dlq_monitor_lambda.add_event_source(
            lambda_events.SqsEventSource(dlq, batch_size=10)
        )

        dlq_monitor_lambda.add_event_source(
            lambda_events.SqsEventSource(fifo_dlq, batch_size=10)
        )

        # ── Replay Lambda ─────────────────────────────────────────────────────
        replay_lambda = _lambda.Function(
            self, "ReplayLambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset("lambda"),
            handler="replay.lambda_handler",
            timeout=Duration.seconds(300),
            role=lambda_role,
            log_retention=logs.RetentionDays.ONE_WEEK,
            environment={
                "DLQ_URL": dlq.queue_url,
                "MAIN_QUEUE_URL": main_queue.queue_url,
            },
        )

        dlq.grant_consume_messages(replay_lambda)
        main_queue.grant_send_messages(replay_lambda)

        # ── Stack outputs ─────────────────────────────────────────────────────
        CfnOutput(self, "MainQueueURL",
                  value=main_queue.queue_url,
                  description="Standard main queue URL")
        CfnOutput(self, "DLQURL",
                  value=dlq.queue_url,
                  description="Standard DLQ URL")
        CfnOutput(self, "FIFOMainQueueURL",
                  value=fifo_main_queue.queue_url,
                  description="FIFO main queue URL")
        CfnOutput(self, "FIFODLQURL",
                  value=fifo_dlq.queue_url,
                  description="FIFO DLQ URL")
        CfnOutput(self, "SNSTopicARN",
                  value=sns_topic.topic_arn,
                  description="SNS topic ARN for DLQ alerts")
        CfnOutput(self, "ReplayLambdaName",
                  value=replay_lambda.function_name,
                  description="Invoke this Lambda to replay DLQ messages")
