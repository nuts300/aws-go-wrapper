package firehose

import (
	"encoding/json"
	"github.com/pkg/errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/firehose"
)

// AWSFirehose is interface of aws firehose
type AWSFirehose interface {
	PutRecord(input *firehose.PutRecordInput) (*firehose.PutRecordOutput, error)
}

// WrapperFirehose is interface of wrapper of aws firehose
type WrapperFirehose interface {
	PutRecord(message interface{}) (*firehose.PutRecordOutput, error)
}

type firehoseClient struct {
	Client     AWSFirehose
	StreamName string
}

// New is return nwe firehose client
func New(client AWSFirehose, streamName string) WrapperFirehose {
	return &firehoseClient{
		Client:     client,
		StreamName: streamName,
	}
}

func (s *firehoseClient) PutRecord(message interface{}) (*firehose.PutRecordOutput, error) {
	arr, err := json.Marshal(message)
	if err != nil {
		return nil, errors.Wrap(err, "Marshal record failure")
	}
	data := append(arr, "\n"...)

	record := &firehose.Record{
		Data: data,
	}
	input := &firehose.PutRecordInput{
		DeliveryStreamName: aws.String(s.StreamName),
		Record:             record,
	}
	result, err := s.Client.PutRecord(input)
	if err != nil {
		return nil, errors.Wrap(err, "Put record err")
	}
	return result, nil
}

