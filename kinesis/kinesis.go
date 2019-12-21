package kinesis

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/pkg/errors"
)

// AWSKinesis is interface of aws kinesis
type AWSKinesis interface {
	PutRecord(input *kinesis.PutRecordInput) (*kinesis.PutRecordOutput, error)
}

// WrapperKinesis is wrapper of aws kinesis
type WrapperKinesis interface {
	PutRecord(partitionKey string, message interface{}) (*kinesis.PutRecordOutput, error)
}

type wrapperKinesis struct {
	Client AWSKinesis
	StreamName string
}

func (s *wrapperKinesis) PutRecord(partitionKey string, message interface{}) (*kinesis.PutRecordOutput, error) {
	arr, err := json.Marshal(message)
	if err != nil {
		return nil, errors.Wrap(err, "Marshal record failure")
	}
	data := append(arr, "\n"...)

	return s.Client.PutRecord(&kinesis.PutRecordInput{
		StreamName: aws.String(s.StreamName),
		PartitionKey: aws.String(partitionKey),
		Data: data,
	})
}

// New return new kinesis wrapper
func New(client AWSKinesis, streamName string) WrapperKinesis {
	return &wrapperKinesis{
		Client: client,
		StreamName: streamName,
	}
}