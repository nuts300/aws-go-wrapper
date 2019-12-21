package stepfunc

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sfn"
	"github.com/pkg/errors"
)

// AWSStepFunc is interface of AWS step function
type AWSStepFunc interface {
	StartExecution(input *sfn.StartExecutionInput) (*sfn.StartExecutionOutput, error)
}

// WrapperStepFunc is wrapper of AWS step function
type WrapperStepFunc interface {
	StartExecution(stateMachineName string, input interface{}) (*sfn.StartExecutionOutput, error)
}

type wrapperStepFunc struct {
	Client AWSStepFunc
}

// StartExecution is starting process of state machine
func (s *wrapperStepFunc) StartExecution(
	stateMachineName string, input interface{}) (*sfn.StartExecutionOutput, error) {

		inputByte, err := json.Marshal(input)
		if err != nil {
			errMessage := fmt.Sprintf("Json marshal failure in StartExecution input=%+v", input)
			return nil, errors.Wrap(err, errMessage)
		}

		return s.Client.StartExecution(&sfn.StartExecutionInput{
			StateMachineArn: aws.String(stateMachineName),
			Input:           aws.String(string(inputByte)),
		})
}

// New return wrapper client of stepFunction
func New(client AWSStepFunc) WrapperStepFunc {
	return &wrapperStepFunc{
		Client: client,
	}
}
