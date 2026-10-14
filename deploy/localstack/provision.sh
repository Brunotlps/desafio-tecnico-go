#!/bin/bash
# Provisiona filas, redrive e o destino dos eventos de saída no LocalStack.
# Idempotente: pode rodar novamente sem efeito colateral.
set -euo pipefail

REGION="${DEFAULT_REGION:-us-east-1}"

queue_arn() { awslocal sqs get-queue-attributes --queue-url "$1" --attribute-names QueueArn --query 'Attributes.QueueArn' --output text; }

# --- Entrada: fila FIFO de operações e sua DLQ ---------------------------------
# MessageGroupId = walletId (ordem por carteira, paralelismo entre carteiras);
# MessageDeduplicationId = messageId do envelope. ContentBasedDeduplication
# desligado: a deduplicação definitiva é a da aplicação (inbox + idempotência).
DLQ_URL=$(awslocal sqs create-queue --queue-name wager-transactions-dlq.fifo \
  --attributes '{"FifoQueue":"true","ContentBasedDeduplication":"false","MessageRetentionPeriod":"1209600"}' \
  --query QueueUrl --output text)
DLQ_ARN=$(queue_arn "$DLQ_URL")

# Após 5 recebimentos sem remoção, a mensagem vai para a DLQ. A visibilidade de
# 60s cobre o processamento de uma operação com folga.
QUEUE_ATTRS=$(cat <<JSON
{"FifoQueue":"true","ContentBasedDeduplication":"false","VisibilityTimeout":"60","MessageRetentionPeriod":"345600",
 "RedrivePolicy":"{\\"deadLetterTargetArn\\":\\"${DLQ_ARN}\\",\\"maxReceiveCount\\":\\"5\\"}"}
JSON
)
awslocal sqs create-queue --queue-name wager-transactions.fifo --attributes "$QUEUE_ATTRS" >/dev/null

# --- Saída: tópico SNS FIFO de eventos + fila de auditoria assinante -----------
# O publisher de outbox publicará em wager-events.fifo com MessageGroupId =
# walletId (ordem por carteira) e MessageDeduplicationId = eventId.
TOPIC_ARN=$(awslocal sns create-topic --name wager-events.fifo \
  --attributes '{"FifoTopic":"true","ContentBasedDeduplication":"false"}' --query TopicArn --output text)
AUDIT_URL=$(awslocal sqs create-queue --queue-name wager-events-audit.fifo \
  --attributes '{"FifoQueue":"true","ContentBasedDeduplication":"false"}' --query QueueUrl --output text)
AUDIT_ARN=$(queue_arn "$AUDIT_URL")
awslocal sns subscribe --topic-arn "$TOPIC_ARN" --protocol sqs --notification-endpoint "$AUDIT_ARN" \
  --attributes '{"RawMessageDelivery":"true"}' >/dev/null

echo "provisioned: wager-transactions.fifo (+dlq, redrive after 5), wager-events.fifo -> wager-events-audit.fifo (region $REGION)"
