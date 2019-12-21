package dynamo

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/pkg/errors"
)

// WrapperDynamo is wrapper of client of aws dynamodb
type WrapperDynamo interface {
	GetItem(tableName string, key string, val string) (*dynamodb.GetItemOutput, error)
	PutItem(tableName string, record interface{}) (*dynamodb.PutItemOutput, error)
}

// AWSDynamo is interface of aws dynamodb
type AWSDynamo interface {
	GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
	PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
}

type wrapperDynamo struct {
	Client AWSDynamo
}

func (s *wrapperDynamo) GetItem(tableName string, key string, val string) (*dynamodb.GetItemOutput, error) {
	return s.Client.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			key: {
				S: aws.String(val),
			},
		},
	})
}

func (s *wrapperDynamo) PutItem(tableName string, record interface{}) (*dynamodb.PutItemOutput, error) {
	item, err := dynamodbattribute.MarshalMap(record)
	if err != nil {
		errMessage := fmt.Sprintf("Marshal put item failure record=%+v", record)
		return nil, errors.Wrap(err, errMessage)
	}
	result, err := s.Client.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      item,
	})
	if err != nil {
		errMessage := fmt.Sprintf("Marshal put item failure item=%+v", item)
		return nil, errors.Wrap(err, errMessage)
	}
	return result, errors.Wrap(err, "Put item failure")
}

// New is return instance of wrapper of dynamodb client
func New(client AWSDynamo) WrapperDynamo {
	return &wrapperDynamo{
		Client: client,
	}
}
