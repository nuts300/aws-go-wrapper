package ssm

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ssm"
)

// AWSSSM is interface of aws athena
type AWSSSM interface {
	GetParameters(input *ssm.GetParametersInput) (*ssm.GetParametersOutput, error)
}

// WrapperSSM is wrapper of aws athena
type WrapperSSM interface {
	GetParameters(names []string, withDecription bool) ([]*ssm.Parameter, error)
}

type wrapperSSM struct {
	Client AWSSSM
}

func (s *wrapperSSM) GetParameters(names []string, withDecription bool) ([]*ssm.Parameter, error) {
	result, err := s.Client.GetParameters(&ssm.GetParametersInput{
		Names: aws.StringSlice(names),
		WithDecryption: aws.Bool(withDecription),
	})
	if err != nil {
		return nil, err
	}

	return result.Parameters, nil
}


// New is return new WrapperSSM
func New(client AWSSSM) WrapperSSM {
	return &wrapperSSM{
		Client: client,
	}
}
