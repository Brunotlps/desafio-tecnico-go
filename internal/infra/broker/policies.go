package broker

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type sqsAttr = types.QueueAttributeName

type policyDoc struct {
	Version   string      `json:"Version"`
	Statement []statement `json:"Statement"`
}

type statement struct {
	Sid       string                       `json:"Sid,omitempty"`
	Effect    string                       `json:"Effect"`
	Principal any                          `json:"Principal,omitempty"`
	Action    []string                     `json:"Action"`
	Resource  []string                     `json:"Resource"`
	Condition map[string]map[string]string `json:"Condition,omitempty"`
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("broker: marshal policy: %v", err))
	}
	return string(b)
}

func allow(sid string, actions []string, resources ...string) statement {
	return statement{Sid: sid, Effect: "Allow", Action: actions, Resource: resources}
}

// policyFor devolve a política mínima de cada responsabilidade, por ação e por recurso. Nenhuma usa
// curinga de ação ou de recurso. Nobody não tem política.
func policyFor(role Role, r Resources) string {
	var st []statement
	switch role {
	case Producer:
		// Produtor interno confiável: só envia à entrada. Sem consumo, remoção, DLQ ou SNS.
		st = []statement{allow("SendToTheInputQueue", []string{"sqs:SendMessage", "sqs:GetQueueUrl"}, r.InputARN)}
	case Runtime:
		// Consumidor: ciclo de vida da mensagem na entrada, envio à DLQ (encaminhamento manual) e leitura da profundidade da DLQ.
		st = []statement{
			allow("ConsumeTheInputQueue", []string{"sqs:GetQueueUrl", "sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility"}, r.InputARN),
			// GetQueueAttributes só na DLQ: a leitura aproximada da sua profundidade para a métrica (nada de inspector/admin).
			allow("ForwardToTheDLQ", []string{"sqs:GetQueueUrl", "sqs:SendMessage", "sqs:GetQueueAttributes"}, r.DLQARN),
		}
	case Publisher:
		// Publisher: publica no tópico de saída; GetTopicAttributes para descobrir/validar o tópico.
		st = []statement{allow("PublishToTheEventTopic", []string{"sns:Publish", "sns:GetTopicAttributes"}, r.TopicARN)}
	case Inspector:
		// Inspeção e preparação de cenários nas filas de DLQ e de auditoria; leitura de atributos da entrada.
		st = []statement{
			allow("InspectAndPrepareDLQAndAudit", []string{"sqs:GetQueueUrl", "sqs:GetQueueAttributes", "sqs:ReceiveMessage",
				"sqs:DeleteMessage", "sqs:ChangeMessageVisibility", "sqs:PurgeQueue", "sqs:SendMessage"}, r.DLQARN, r.AuditARN),
			allow("ReadInputQueueAttributes", []string{"sqs:GetQueueUrl", "sqs:GetQueueAttributes"}, r.InputARN),
			allow("ReadTopicSubscriptions", []string{"sns:GetTopicAttributes", "sns:ListSubscriptionsByTopic"}, r.TopicARN),
		}
	default:
		return ""
	}
	return mustJSON(policyDoc{Version: "2012-10-17", Statement: st})
}
