package ssm

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ssm"
	"testing"
)

type SSMMock struct {}

func (s *SSMMock) GetParameters(input *ssm.GetParametersInput) (*ssm.GetParametersOutput, error) {
	return &ssm.GetParametersOutput{
		Parameters: []*ssm.Parameter{
			{
				Name: aws.String("dummyParam1"),
				Value: aws.String("dummyParamValue1"),
			},
			{
				Name: aws.String("dummyParam2"),
				Value: aws.String("dummyParamValue2"),
			},
		},
	}, nil
}

func TestGetParameters(t *testing.T) {
	ssmMock := &SSMMock{}

	ssmClient := New(ssmMock)
	params, err := ssmClient.GetParameters([]string{"dummyParam1", "dummyParam2"}, false)
	if err != nil {
		t.Fatalf("GetParameters failure %s", err.Error())
	}

	for _, param := range params {
		name := aws.StringValue(param.Name)
		value := aws.StringValue(param.Value)
		if name == "dummyParam1" && value != "dummyParamValue1" {
			t.Fatalf("Wrong param value name:%s value:%s", name, value)
		}
		if name == "dummyParam2" && value != "dummyParamValue2" {
			t.Fatalf("Wrong param value name:%s value:%s", name, value)
		}
	}
}