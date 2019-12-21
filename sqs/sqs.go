package sqs

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
)

// AWSSQS is interface of aws sqs
type AWSSQS interface {
	ReceiveMessage(input *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(input *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error)
	SendMessage(input *sqs.SendMessageInput) (*sqs.SendMessageOutput, error)
}

// WrapperSQS is wrapper of aws sqs
type WrapperSQS interface {
	ReceiveMessage() (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(receiptHandle *string) error
	SendMessage(message interface{}) (messageID *string, err error)
}

type wrapperSQS struct {
	Client   AWSSQS
	QueueURL string
	QueueMessageGroupID string
}

func (s *wrapperSQS) ReceiveMessage() (*sqs.ReceiveMessageOutput, error) {
	params := &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(s.QueueURL),
	}
	return s.Client.ReceiveMessage(params)
}

func (s *wrapperSQS) DeleteMessage(receiptHandle *string) error {
	_, err := s.Client.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(s.QueueURL),
		ReceiptHandle: receiptHandle,
	})
	return err
}

func (s *wrapperSQS) SendMessage(message interface{}) (messageID *string, err error) {
	jsonBytes, err := json.Marshal(message)
	if err != nil {
		return nil, errors.Wrap(err, "Json marshal failure")
	}
	input := &sqs.SendMessageInput{
		QueueUrl:       aws.String(s.QueueURL),
		MessageBody:    aws.String(string(jsonBytes)),
	}
	if s.QueueMessageGroupID != "" {
		input.MessageGroupId = aws.String(s.QueueMessageGroupID)
	}

	sendMessageOutput, err := s.Client.SendMessage(input)
	if err != nil {
		return nil, errors.Wrap(err, "Send message failure")
	}

	return sendMessageOutput.MessageId, nil
}

// New is return new sqs client
func New(client AWSSQS, queueURL string, messageGroupID string) WrapperSQS {
	return &wrapperSQS{
		Client:   client,
		QueueURL: queueURL,
		QueueMessageGroupID: messageGroupID,
	}
}
