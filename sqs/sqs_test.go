package sqs

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/service/sqs"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
)

type SQSMock struct {
}

type DummyData struct {
	JobID    string   `json:"jobId"`
}

func (s *SQSMock) ReceiveMessage(input *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	return &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{
			{
				Body: aws.String(`{ "jobId":"dummyJobId" }`),
			},
			{
				Body: aws.String(`{ "jobId":"dummyJobId" }`),
			},
		},
	}, nil
}

func (s *SQSMock) DeleteMessage(input *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
	return &sqs.DeleteMessageOutput{}, nil
}

func (s *SQSMock) SendMessage(input *sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
	return &sqs.SendMessageOutput{
		MessageId: aws.String("dummyMessageId"),
	}, nil
}

func TestReceiveMessage(t *testing.T) {
	queueName := "test-queue"
	sqsMock := &SQSMock{}

	sqsClient := New(sqsMock, queueName, "")
	result, err := sqsClient.ReceiveMessage()
	if err != nil {
		t.Fatalf("Receive message failure %s", err.Error())
	}

	for _, m := range result.Messages {
		bodyBytes := []byte(aws.StringValue(m.Body))
		var messageData = &DummyData{}
		if err := json.Unmarshal(bodyBytes, messageData); err != nil {
			t.Fatalf("Unmarshal message data failure %s", err.Error())
		}
		if messageData.JobID != "dummyJobId" {
			t.Fatalf("Wrong jobId %s", messageData.JobID)
		}
	}
}

func TestDeleteMessage(t *testing.T) {
	queueName := "test-queue"
	sqsMock := &SQSMock{}

	sqsClient := New(sqsMock, queueName, "")
	err := sqsClient.DeleteMessage(aws.String("dummyReceiptHandle"))
	if err != nil {
		t.Fatalf("Raise error %s", err.Error())
	}
}

func TestSendMessage(t *testing.T) {
	awsSQSMock := &SQSMock{}
	sqsMock := New(awsSQSMock, "test-queue", "test-group")
	testMessage := &DummyData{
		JobID: "dummyJobID",
	}
	_, err := sqsMock.SendMessage(testMessage)
	if err != nil {
		t.Fatalf("Raise error %s", err.Error())
	}
}
