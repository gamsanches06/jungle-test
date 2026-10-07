#!/bin/bash
# LocalStack "ready" hook: provisions the SQS FIFO queues, their DLQs and
# redrive policies, plus IAM identities/policies describing least privilege
# per broker role. Idempotent (create-queue returns the existing queue).
set -euo pipefail

REGION="${AWS_DEFAULT_REGION:-us-east-1}"
ACCOUNT="000000000000"
MAX_RECEIVES="${SQS_MAX_RECEIVES:-5}"
VISIBILITY="${SQS_VISIBILITY_TIMEOUT_SECONDS:-30}"

create_fifo() { # name attributes-json
  awslocal sqs create-queue --region "$REGION" --queue-name "$1" --attributes "$2" --query QueueUrl --output text
}
arn_of() {
  awslocal sqs get-queue-attributes --region "$REGION" --queue-url "$1" --attribute-names QueueArn --query Attributes.QueueArn --output text
}

DLQ_URL=$(create_fifo wager-transactions-dlq.fifo '{"FifoQueue":"true","ContentBasedDeduplication":"false","MessageRetentionPeriod":"1209600"}')
DLQ_ARN=$(arn_of "$DLQ_URL")
IN_URL=$(create_fifo wager-transactions.fifo "{\"FifoQueue\":\"true\",\"ContentBasedDeduplication\":\"false\",\"VisibilityTimeout\":\"${VISIBILITY}\",\"MessageRetentionPeriod\":\"345600\",\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"${DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"${MAX_RECEIVES}\\\"}\"}")
IN_ARN=$(arn_of "$IN_URL")

# Outgoing integration events + DLQ for downstream consumers.
EV_DLQ_URL=$(create_fifo wallet-events-dlq.fifo '{"FifoQueue":"true","ContentBasedDeduplication":"false","MessageRetentionPeriod":"1209600"}')
EV_DLQ_ARN=$(arn_of "$EV_DLQ_URL")
EV_URL=$(create_fifo wallet-events.fifo "{\"FifoQueue\":\"true\",\"ContentBasedDeduplication\":\"false\",\"MessageRetentionPeriod\":\"345600\",\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"${EV_DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"10\\\"}\"}")
EV_ARN=$(arn_of "$EV_URL")

# Broker identities and policies (least privilege per role).
policy() { # name json
  awslocal iam create-policy --policy-name "$1" --policy-document "$2" >/dev/null 2>&1 || true
}
user() { # name policy
  awslocal iam create-user --user-name "$1" >/dev/null 2>&1 || true
  awslocal iam attach-user-policy --user-name "$1" --policy-arn "arn:aws:iam::${ACCOUNT}:policy/$2" >/dev/null 2>&1 || true
}
policy wager-producer "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"sqs:SendMessage\",\"sqs:GetQueueUrl\"],\"Resource\":\"${IN_ARN}\"}]}"
policy wager-consumer "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"sqs:ReceiveMessage\",\"sqs:DeleteMessage\",\"sqs:ChangeMessageVisibility\",\"sqs:GetQueueAttributes\",\"sqs:GetQueueUrl\"],\"Resource\":\"${IN_ARN}\"},{\"Effect\":\"Allow\",\"Action\":[\"sqs:SendMessage\",\"sqs:GetQueueUrl\"],\"Resource\":\"${DLQ_ARN}\"}]}"
policy wallet-events-publisher "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"sqs:SendMessage\",\"sqs:GetQueueUrl\"],\"Resource\":\"${EV_ARN}\"}]}"
user wager-producer wager-producer
user wager-consumer wager-consumer
user wallet-events-publisher wallet-events-publisher

# Queue (resource) policies: only the role identities may use each queue.
awslocal sqs set-queue-attributes --queue-url "$IN_URL" --attributes "{\"Policy\":\"{\\\"Version\\\":\\\"2012-10-17\\\",\\\"Statement\\\":[{\\\"Effect\\\":\\\"Allow\\\",\\\"Principal\\\":{\\\"AWS\\\":\\\"arn:aws:iam::${ACCOUNT}:user/wager-producer\\\"},\\\"Action\\\":\\\"sqs:SendMessage\\\",\\\"Resource\\\":\\\"${IN_ARN}\\\"},{\\\"Effect\\\":\\\"Allow\\\",\\\"Principal\\\":{\\\"AWS\\\":\\\"arn:aws:iam::${ACCOUNT}:user/wager-consumer\\\"},\\\"Action\\\":[\\\"sqs:ReceiveMessage\\\",\\\"sqs:DeleteMessage\\\",\\\"sqs:ChangeMessageVisibility\\\",\\\"sqs:GetQueueAttributes\\\"],\\\"Resource\\\":\\\"${IN_ARN}\\\"}]}\"}"
awslocal sqs set-queue-attributes --queue-url "$EV_URL" --attributes "{\"Policy\":\"{\\\"Version\\\":\\\"2012-10-17\\\",\\\"Statement\\\":[{\\\"Effect\\\":\\\"Allow\\\",\\\"Principal\\\":{\\\"AWS\\\":\\\"arn:aws:iam::${ACCOUNT}:user/wallet-events-publisher\\\"},\\\"Action\\\":\\\"sqs:SendMessage\\\",\\\"Resource\\\":\\\"${EV_ARN}\\\"}]}\"}"

echo "SQS provisioned: $IN_URL (dlq $DLQ_URL), events $EV_URL"
