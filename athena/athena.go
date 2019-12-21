package athena

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/pkg/errors"
)

// AWSAthena is interface of aws athena
type AWSAthena interface {
	GetQueryExecution(input *athena.GetQueryExecutionInput) (*athena.GetQueryExecutionOutput, error)
	StartQueryExecution(input *athena.StartQueryExecutionInput) (*athena.StartQueryExecutionOutput, error)
	GetQueryResultsPages(input *athena.GetQueryResultsInput, fn func(*athena.GetQueryResultsOutput, bool) bool) error
	GetQueryResults(input *athena.GetQueryResultsInput) (*athena.GetQueryResultsOutput, error)
}

// WrapperAthena is wrapper of aws athena
type WrapperAthena interface {
	CheckJobStatus(execID string) (*athena.GetQueryExecutionOutput, error)
	ExecuteQuery(query string, dest string) (*athena.StartQueryExecutionOutput, error)
	WaitJobStatus(jobID string, durationSecond int) (bool, error)
	GetResults(jobID string, nextToken string, maxResutls int) (*athena.GetQueryResultsOutput, error)
	GetResultsPages(jobID string, maxResults int, pageCh chan *athena.GetQueryResultsOutput) error
}

type wrapperAthena struct {
	Client       AWSAthena
	DatabaseName string
}

// New is return new AthenaClient
func New(client AWSAthena, dbName string) WrapperAthena {
	return &wrapperAthena{
		Client:       client,
		DatabaseName: dbName,
	}
}

func (s *wrapperAthena) CheckJobStatus(execID string) (*athena.GetQueryExecutionOutput, error) {
	return s.Client.GetQueryExecution(&athena.GetQueryExecutionInput{
		QueryExecutionId: aws.String(execID),
	})
}

func (s *wrapperAthena) ExecuteQuery(query string, dest string) (*athena.StartQueryExecutionOutput, error) {
	input := &athena.StartQueryExecutionInput{
		QueryExecutionContext: &athena.QueryExecutionContext{
			Database: aws.String(s.DatabaseName),
		},
		QueryString: aws.String(query),
		ResultConfiguration: &athena.ResultConfiguration{
			OutputLocation: aws.String(dest),
		},
	}
	return s.Client.StartQueryExecution(input)
}

func (s *wrapperAthena) WaitJobStatus(jobID string, durationSecond int) (bool, error) {
	for {
		status, statusErr := s.CheckJobStatus(jobID)
		if statusErr != nil {
			return false, statusErr
		}
		// The state of query execution. QUEUED state is listed but is not used by Athena
		// and is reserved for future use. RUNNING indicates that the query has been
		// submitted to the service, and Athena will execute the query as soon as resources
		// are available. SUCCEEDED indicates that the query completed without errors.
		// FAILED indicates that the query experienced an error and did not complete
		// processing. CANCELLED indicates that a user input interrupted query execution.
		state := aws.StringValue(status.QueryExecution.Status.State)
		fmt.Println(fmt.Sprintf("Wait job status=%s",state))
		switch state {
		case "SUCCEEDED":
			return true, nil
		case "RUNNING":
			time.Sleep(time.Duration(durationSecond*1000) * time.Millisecond)
			continue
		case "CANCELLED":
			message := fmt.Sprintf("Job is canceled jobId:%s", jobID)
			return false, errors.New(message)
		case "FAILED":
			message := fmt.Sprintf("Job is failed jobId:%s", jobID)
			return false, errors.New(message)
		default:
			message := fmt.Sprintf("Invalid state code jobId:%s", jobID)
			return false, errors.New(message)
		}
	}
}

func (s *wrapperAthena) GetResults(jobID string, nextToken string, maxResults int) (*athena.GetQueryResultsOutput, error) {
	input := &athena.GetQueryResultsInput{
		MaxResults: aws.Int64(int64(maxResults)),
		QueryExecutionId: aws.String(jobID),
	}
	if nextToken != "" {
		input.NextToken = aws.String(nextToken)
	}
	return s.Client.GetQueryResults(input)
}

func (s *wrapperAthena) GetResultsPages(
	jobID string,
	maxResults int,
	pageCh chan *athena.GetQueryResultsOutput) error {

	params := &athena.GetQueryResultsInput{
		QueryExecutionId: aws.String(jobID),
		MaxResults:       aws.Int64(int64(maxResults)),
	}
	err := s.Client.GetQueryResultsPages(params,
		func(page *athena.GetQueryResultsOutput, lastPage bool) bool {
			if lastPage {
				fmt.Println("Reached last page")
				close(pageCh)
				return false
			}
			fmt.Println(fmt.Sprintf("Pass results to result channel nextToken=%s", *page.NextToken))
			pageCh <- page
			return true
		})
	if err != nil {
		close(pageCh)
		return errors.Wrap(err, "Get query results pages failure")
	}
	return nil
}
