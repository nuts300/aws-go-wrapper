package dynamo

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"testing"
)

type TestRecord struct {
	ID          string `json:"id"`
	DescText string `json:"desc_text"`
}

type DynamoMock struct {}

func (s *DynamoMock) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	return &dynamodb.GetItemOutput{
		Item: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String("dummyId"),
			},
			"desc_text": {
				S: aws.String("dummyText"),
			},
		},
	}, nil
}

func (s *DynamoMock) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	return &dynamodb.PutItemOutput{

	}, nil
}

func TestPutItem(t *testing.T) {
	dummyTableName := "test-table"

	dynamoMock := &DynamoMock{}
	dynamoDBClient := New(dynamoMock)

	_, err := dynamoDBClient.PutItem(dummyTableName, TestRecord{
		ID:          "dummyId",
		DescText: "dummyDatetime",
	})
	if err != nil {
		t.Fatalf("Put item failure %s", err.Error())
	}

	getResult, err := dynamoDBClient.GetItem("dummyTableNaem", "id", "dymmyID")
	if err != nil {
		t.Fatalf("Get item failure %s", err.Error())
	}

	var record TestRecord
	if err := dynamodbattribute.UnmarshalMap(getResult.Item, &record); err != nil {
		t.Fatalf("Unmarshal record failure %s", err.Error())
	}
	if record.ID != "dummyId" {
		t.Fatalf("Wrong id. Expected %s but %s", "dummyId", record.ID)
	}
	if record.DescText != "dummyText" {
		t.Fatalf("Wrong desc text. Expected %s but %s", "dummyText", record.DescText)
	}
}
