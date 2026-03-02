package opsvc

import (
	"encoding/json"
	"fmt"

	"via-backend/internal/messaging"
)

func OperationSubject(operationID string) string {
	return fmt.Sprintf("ops.operation.%s", operationID)
}

func PublishUpdate(broker *messaging.Broker, op *Operation) error {
	if broker == nil || op == nil || op.ID == "" {
		return nil
	}
	body, err := json.Marshal(op)
	if err != nil {
		return err
	}
	return broker.Publish(OperationSubject(op.ID), body)
}
