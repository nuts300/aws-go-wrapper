package athena

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/athena"
	"testing"
)

type AthenaMock struct{
	GetQueryExecutionOutputCh chan *athena.GetQueryExecutionOutput
	GetQueryResultsOutputCh chan *athena.GetQueryResultsOutput
	GetQueryResultsOutputCounter int
}

func (s *AthenaMock) GetQueryExecution(input *athena.GetQueryExecutionInput) (*athena.GetQueryExecutionOutput, error) {
	return <-s.GetQueryExecutionOutputCh, nil
}

func (s *AthenaMock) StartQueryExecution(
	input *athena.StartQueryExecutionInput) (*athena.StartQueryExecutionOutput, error) {
		return &athena.StartQueryExecutionOutput{
			QueryExecutionId: aws.String("dummyQueryID"),
		}, nil

	}

func (s *AthenaMock) GetQueryResultsPages(
	input *athena.GetQueryResultsInput, fn func(*athena.GetQueryResultsOutput, bool) bool) error {
		isContinue := true
		if s.GetQueryResultsOutputCounter > 0 {
			isContinue = false
		}
		fn(<-s.GetQueryResultsOutputCh, isContinue)
		s.GetQueryResultsOutputCounter++
		return nil
	}

func (s *AthenaMock) GetQueryResults(input *athena.GetQueryResultsInput) (*athena.GetQueryResultsOutput, error) {
	return &athena.GetQueryResultsOutput{
		NextToken: aws.String("dummyToken"),
		ResultSet: &athena.ResultSet{
			Rows: []*athena.Row{
				{
					Data: []*athena.Datum{
						{
							VarCharValue: aws.String("dummyData1"),
						},
						{
							VarCharValue: aws.String("dummyData2"),
						},
					},
				},
				{
					Data: []*athena.Datum{
						{
							VarCharValue: aws.String("dummyData3"),
						},
						{
							VarCharValue: aws.String("dummyData4"),
						},
					},
				},
			},
		},
	}, nil
}

func (s *AthenaMock) CloseCh() {
	close(s.GetQueryExecutionOutputCh)
	close(s.GetQueryResultsOutputCh)
}

func makeGetQueryExecutionCh() chan *athena.GetQueryExecutionOutput {
	outputCh := make(chan *athena.GetQueryExecutionOutput, 2)
	outputCh <-  &athena.GetQueryExecutionOutput{
		QueryExecution: &athena.QueryExecution{
			Query: aws.String("dummyQuery"),
			QueryExecutionId: aws.String("dummyQueryID"),
			Status: &athena.QueryExecutionStatus{
				State: aws.String("RUNNING"),
			},
		},
	}
	outputCh <- &athena.GetQueryExecutionOutput{
		QueryExecution: &athena.QueryExecution{
			Query: aws.String("dummyQuery"),
			QueryExecutionId: aws.String("dummyQueryID"),
			Status: &athena.QueryExecutionStatus{
				State: aws.String("SUCCEEDED"),
			},
		},
	}
	return outputCh
}

func makeGetQueryResultsOutputCh() chan *athena.GetQueryResultsOutput {
	outputCh := make(chan *athena.GetQueryResultsOutput, 2)
	outputCh <- &athena.GetQueryResultsOutput{
		NextToken: aws.String("dummyToken"),
		ResultSet: &athena.ResultSet{
			Rows: []*athena.Row{
				{
					Data: []*athena.Datum{
						{
							VarCharValue: aws.String("dummyData1"),
						},
						{
							VarCharValue: aws.String("dummyData2"),
						},
					},
				},
				{
					Data: []*athena.Datum{
						{
							VarCharValue: aws.String("dummyData3"),
						},
						{
							VarCharValue: aws.String("dummyData4"),
						},
					},
				},
			},
		},
	}

	outputCh <- &athena.GetQueryResultsOutput{
		NextToken: aws.String("dummyToken"),
		ResultSet: &athena.ResultSet{
			Rows: []*athena.Row{
				{
					Data: []*athena.Datum{
						{
							VarCharValue: aws.String("dummyData5"),
						},
						{
							VarCharValue: aws.String("dummyData6"),
						},
					},
				},
			},
		},
	}
	return outputCh
}


func TestExecuteQuery(t *testing.T) {

	athenaMock := &AthenaMock{
		GetQueryExecutionOutputCh: makeGetQueryExecutionCh(),
		GetQueryResultsOutputCh: makeGetQueryResultsOutputCh(),
	}
	athenaWrapper := New(athenaMock, "test-db")
	result , err := athenaWrapper.ExecuteQuery("dummyQuery", "test-dest")
	if err != nil {
		t.Fatalf("Execute failuer %s", err.Error())
	}
	if *result.QueryExecutionId != "dummyQueryID" {
		t.Fatalf("Wrong execution id: %s", *result.QueryExecutionId)
	}
	defer athenaMock.CloseCh()
}


func TestCheckJobStatus(t *testing.T) {
	athenaMock := &AthenaMock{
		GetQueryExecutionOutputCh: makeGetQueryExecutionCh(),
		GetQueryResultsOutputCh: makeGetQueryResultsOutputCh(),
	}
	athenaWrapper := New(athenaMock, "test-db")
	result, err := athenaWrapper.CheckJobStatus("dummyID")
	if err != nil {
		t.Fatalf("Check job status failure. %s", err.Error())
	}
	if *result.QueryExecution.Status.State != "RUNNING" {
		t.Fatalf("Wrong state %s", *result.QueryExecution.Status.State)
	}

	result, err = athenaWrapper.CheckJobStatus("dummyID")
	if err != nil {
		t.Fatalf("Check job status failure. %s", err.Error())
	}
	if *result.QueryExecution.Status.State != "SUCCEEDED" {
		t.Fatalf("Wrong state %s", *result.QueryExecution.Status.State)
	}

	defer athenaMock.CloseCh()
}

func TestWaitJobStatus(t *testing.T) {
	athenaMock := &AthenaMock{
		GetQueryExecutionOutputCh: makeGetQueryExecutionCh(),
		GetQueryResultsOutputCh: makeGetQueryResultsOutputCh(),
	}
	athenaWrapper := New(athenaMock, "test-db")
	result, err := athenaWrapper.WaitJobStatus("dummyID", 1)
	if err != nil {
		t.Fatalf("Wait job status failure. %s", err.Error())
	}
	if result != true {
		t.Fatalf("Wrong result %t", result)
	}

	defer athenaMock.CloseCh()
}


func TestGetResults(t *testing.T) {
	athenaMock := &AthenaMock{
		GetQueryExecutionOutputCh: makeGetQueryExecutionCh(),
		GetQueryResultsOutputCh: makeGetQueryResultsOutputCh(),
	}
	athenaWrapper := New(athenaMock, "test-db")
	result, err := athenaWrapper.GetResults("dummyJob","dummyToken",1 )
	if err != nil {
		t.Fatalf("Get results failure. %s", err.Error())
	}
	if len(result.ResultSet.Rows) != 2 {
		t.Log("result rows", result.ResultSet.Rows)
		t.Fatalf("Wrong result rows length:%d", len(result.ResultSet.Rows))
	}
	if *result.NextToken != "dummyToken" {
		t.Fatalf("Wrong next token: %s", *result.NextToken)
	}

	defer athenaMock.CloseCh()
}

func TestGetResultsPages(t *testing.T) {
	athenaMock := &AthenaMock{
		GetQueryExecutionOutputCh: makeGetQueryExecutionCh(),
		GetQueryResultsOutputCh: makeGetQueryResultsOutputCh(),
	}
	athenaWrapper := New(athenaMock, "test-db")
	pageCh := make(chan *athena.GetQueryResultsOutput)
	athenaWrapper.GetResultsPages("dymmyJob", 1, pageCh)

	for page := range pageCh {
		t.Logf("page's rows:%+v", page.ResultSet.Rows)
	}

	if athenaMock.GetQueryResultsOutputCounter != 1 {
		t.Fatalf("Wrong athena mock counter %d", athenaMock.GetQueryResultsOutputCounter)
	}

	defer athenaMock.CloseCh()
}