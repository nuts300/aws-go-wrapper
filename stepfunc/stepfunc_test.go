package stepfunc

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sfn"
	"testing"
	"time"
)

type StepFuncMock struct {}

type DummyData struct {
	UserIDs []string `json:"user_ids"`
}

func (s *StepFuncMock) StartExecution(input *sfn.StartExecutionInput) (*sfn.StartExecutionOutput, error) {
	return &sfn.StartExecutionOutput{
		ExecutionArn: aws.String("dummyArn"),
		StartDate: aws.Time(time.Now()),
	}, nil
}


func TestStartExecution(t *testing.T) {
	stepFuncMock := &StepFuncMock{}
	stepFuncWrapper := New(stepFuncMock)
	testMessage := &DummyData{
		UserIDs: []string{"1", "2"},
	}
	result, err := stepFuncWrapper.StartExecution("dummyStateMachine", testMessage)
	if err != nil {
		t.Fatalf("Start execution failure %s", err.Error())
	}
	if *result.ExecutionArn != "dummyArn" {
		t.Fatalf("Wrong execution arn %s", *result.ExecutionArn)
	}
}
