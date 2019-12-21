package kinesis

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"testing"
)

type KinesisMock struct {}

func (s *KinesisMock) PutRecord(input *kinesis.PutRecordInput) (*kinesis.PutRecordOutput, error) {
	return &kinesis.PutRecordOutput{

	}, nil
}

func TestPutRecord(t *testing.T) {

	kinesisMock := &KinesisMock{}
	dummyStreamName := "test-user-stream"

	kinesisClient := New(kinesisMock, dummyStreamName)

	dummyMessage := struct {
		Title string
		Text  string
	}{
		Title: "dummyTitle",
		Text:  "dummyTest",
	}
	fmt.Println(fmt.Sprintf("Put dummyMessage to firehose %+v", dummyMessage))
	_, err := kinesisClient.PutRecord("dummyPartition", &dummyMessage)
	if err != nil {
		t.Fatalf("Put record failure %s", err.Error())
	}
}


