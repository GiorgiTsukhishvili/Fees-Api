package bills

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

type WorkflowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

func TestWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(WorkflowTestSuite))
}

func (s *WorkflowTestSuite) newEnv() *testsuite.TestWorkflowEnvironment {
	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(RecordLineItemActivity, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(RecordBillClosedActivity, mock.Anything, mock.Anything).Return(nil)
	return env
}

func (s *WorkflowTestSuite) Test_FullLifecycle() {
	env := s.newEnv()

	var addResult1, addResult2, addResultDup AddLineItemResult
	var addErr, dupErr, crossCurrencyErr, addAfterCloseErr, closeErr error
	var closeResult Bill

	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow("AddLineItem", "u1", &testsuite.TestUpdateCallback{
			OnAccept: func() {},
			OnReject: func(err error) { addErr = err },
			OnComplete: func(r any, err error) {
				addErr = err
				if err == nil {
					addResult1 = r.(AddLineItemResult)
				}
			},
		}, AddLineItemInput{IdempotencyKey: "k1", Description: "fee 1", Amount: Money{AmountMinor: 500, Currency: USD}})
	}, time.Second)

	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow("AddLineItem", "u2", &testsuite.TestUpdateCallback{
			OnAccept: func() {},
			OnReject: func(err error) { addErr = err },
			OnComplete: func(r any, err error) {
				addErr = err
				if err == nil {
					addResult2 = r.(AddLineItemResult)
				}
			},
		}, AddLineItemInput{IdempotencyKey: "k2", Description: "fee 2", Amount: Money{AmountMinor: 250, Currency: USD}})
	}, 2*time.Second)

	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow("AddLineItem", "u3", &testsuite.TestUpdateCallback{
			OnAccept: func() {},
			OnReject: func(err error) { dupErr = err },
			OnComplete: func(r any, err error) {
				dupErr = err
				if err == nil {
					addResultDup = r.(AddLineItemResult)
				}
			},
		}, AddLineItemInput{IdempotencyKey: "k1", Description: "fee 1 retried", Amount: Money{AmountMinor: 500, Currency: USD}})
	}, 3*time.Second)

	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow("AddLineItem", "u4", &testsuite.TestUpdateCallback{
			OnAccept:   func() { s.Fail("cross-currency line item must not be accepted") },
			OnReject:   func(err error) { crossCurrencyErr = err },
			OnComplete: func(r any, err error) {},
		}, AddLineItemInput{IdempotencyKey: "k4", Description: "wrong currency", Amount: Money{AmountMinor: 100, Currency: GEL}})
	}, 4*time.Second)

	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow("CloseBill", "u5", &testsuite.TestUpdateCallback{
			OnAccept: func() {},
			OnReject: func(err error) { closeErr = err },
			OnComplete: func(r any, err error) {
				closeErr = err
				if err == nil {
					closeResult = r.(Bill)
				}
			},
		}, nil)

		// Sent synchronously right after, in the same tick, so the
		// workflow (which completes the instant CloseBill succeeds)
		// hasn't had a chance to stop the test clock yet.
		env.UpdateWorkflow("AddLineItem", "u6", &testsuite.TestUpdateCallback{
			OnAccept:   func() { s.Fail("line item must not be accepted on a closed bill") },
			OnReject:   func(err error) { addAfterCloseErr = err },
			OnComplete: func(r any, err error) {},
		}, AddLineItemInput{IdempotencyKey: "k6", Description: "too late", Amount: Money{AmountMinor: 100, Currency: USD}})
	}, 5*time.Second)

	env.ExecuteWorkflow(BillWorkflow, CreateBillInput{
		BillID: "bill-1", AccountID: "acct-1", PeriodID: "2026-09", Currency: USD,
	})

	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())

	var finalResult Bill
	s.Require().NoError(env.GetWorkflowResult(&finalResult))

	s.NoError(addErr)
	s.Equal(int64(500), addResult1.RunningTotal.AmountMinor)
	s.Equal(int64(750), addResult2.RunningTotal.AmountMinor)

	s.NoError(dupErr)
	s.Equal(addResult1, addResultDup, "a retried idempotency key must return the original result, not double-add")

	s.Error(crossCurrencyErr)

	s.NoError(closeErr)
	s.Equal(StatusClosed, closeResult.Status)
	s.Equal(int64(750), closeResult.Total.AmountMinor)
	s.Len(closeResult.LineItems, 2)

	s.Error(addAfterCloseErr, "a line item must be rejected once the bill is closed")

	s.Equal(StatusClosed, finalResult.Status)
	s.Equal(int64(750), finalResult.Total.AmountMinor)
}

func (s *WorkflowTestSuite) Test_RejectsInvalidCurrencyOnCreate() {
	env := s.newEnv()

	env.ExecuteWorkflow(BillWorkflow, CreateBillInput{
		BillID: "bill-2", AccountID: "acct-1", PeriodID: "2026-09", Currency: "EUR",
	})

	s.True(env.IsWorkflowCompleted())

	var appErr *temporal.ApplicationError
	s.Require().True(errors.As(env.GetWorkflowError(), &appErr))
	s.Equal(ErrTypeInvalidInput, appErr.Type())
}
