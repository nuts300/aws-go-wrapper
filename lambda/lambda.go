package lambda

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/lambda"
)

type AWSLambda interface {
	Invoke(input *lambda.InvokeInput) (*lambda.InvokeOutput, error)
}

type WrapperLambda interface {
	Invoke(funcName string, payload []byte, queries map[string]string, sync bool) (*lambda.InvokeOutput, error)
}

type wrapperLambda struct {
	Client AWSLambda
}

func makeQuery(queries map[string]string) string {
	queryStr := ""
	for k, v := range queries {
		splitter := "&"
		if queryStr == "" {
			splitter = ""
		}
		queryStr = fmt.Sprintf("%s%s%s=%s", queryStr, splitter, k, v)
	}
	return queryStr
}

func (s *wrapperLambda) Invoke(
	funcName string, payload []byte, queries map[string]string, sync bool) (*lambda.InvokeOutput, error) {

		input := &lambda.InvokeInput{
			FunctionName: aws.String(funcName),
			Payload: payload,
		}
		if len(queries) < 0 {
			input.Qualifier = aws.String(makeQuery(queries))
		}
		if sync {
			input.InvocationType = aws.String("RequestResponse")
		} else {
			input.InvocationType = aws.String("Event")
		}
		return s.Client.Invoke(input)
}

func New(client AWSLambda) WrapperLambda{
	return &wrapperLambda{
		Client: client,
	}
}