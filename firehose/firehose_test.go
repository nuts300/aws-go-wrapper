package firehose

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/firehose"
	"testing"
)

type FirehoseMock struct {}

func (s *FirehoseMock) PutRecord(input *firehose.PutRecordInput) (*firehose.PutRecordOutput, error) {
	return &firehose.PutRecordOutput{
		RecordId: aws.String("dummyRecordID"),
	}, nil
}

func TestPutRecord(t *testing.T) {

	firehoseMock := &FirehoseMock{}
	dummyStreamName := "test-user-stream"

	firehoseClient := New(firehoseMock, dummyStreamName)

	dummyMessage := struct {
		Title string
		Text  string
	}{
		Title: "dummyTitle",
		Text:  "dummyTest",
	}
	fmt.Println(fmt.Sprintf("Put dummyMessage to firehose %+v", dummyMessage))
	result, err := firehoseClient.PutRecord(&dummyMessage)
	if err != nil {
		t.Fatalf("Put record failure %s", err.Error())
	}
	if aws.StringValue(result.RecordId) != "dummyRecordID" {
		t.Fatalf("Wraong record id %s", err.Error())
	}

}

