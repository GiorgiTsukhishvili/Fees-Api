package bills

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"encore.dev/beta/errs"
)

func newTestService(t *testing.T) *Service {
	t.Helper()
	svc, err := initService()
	require.NoError(t, err)
	t.Cleanup(func() { svc.Shutdown(context.Background()) })
	return svc
}

func TestAPI_FullLifecycle(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	// A unique account ID per run: workflow IDs (derived from
	// account+period) persist across test runs against the local dev
	// Postgres/Temporal, and a bill that already closed successfully
	// can't be recreated (by design — see WorkflowIDReusePolicy in
	// CreateBill), so a fixed ID would only pass once.
	accountID := fmt.Sprintf("api-test-acct-%d", time.Now().UnixNano())

	bill, err := svc.CreateBill(ctx, &CreateBillRequest{
		AccountID: accountID,
		PeriodID:  "2026-11",
		Currency:  USD,
	})
	require.NoError(t, err)
	require.Equal(t, StatusOpen, bill.Status)

	_, err = svc.CreateBill(ctx, &CreateBillRequest{
		AccountID: accountID,
		PeriodID:  "2026-11",
		Currency:  USD,
	})
	require.Error(t, err)
	var errsErr *errs.Error
	require.ErrorAs(t, err, &errsErr)
	require.Equal(t, errs.AlreadyExists, errsErr.Code)

	result, err := svc.AddLineItem(ctx, bill.ID, &AddLineItemRequest{
		IdempotencyKey: "api-k1",
		Description:    "fee",
		Amount:         Money{AmountMinor: 100, Currency: USD},
	})
	require.NoError(t, err)
	require.Equal(t, int64(100), result.RunningTotal.AmountMinor)

	closed, err := svc.CloseBill(ctx, bill.ID)
	require.NoError(t, err)
	require.Equal(t, StatusClosed, closed.Status)

	_, err = svc.AddLineItem(ctx, bill.ID, &AddLineItemRequest{
		IdempotencyKey: "api-k2",
		Description:    "too late",
		Amount:         Money{AmountMinor: 100, Currency: USD},
	})
	require.Error(t, err)
	require.ErrorAs(t, err, &errsErr)
	require.Equal(t, errs.Aborted, errsErr.Code)

	got, err := svc.GetBill(ctx, bill.ID)
	require.NoError(t, err)
	require.Equal(t, StatusClosed, got.Status)
	require.Len(t, got.LineItems, 1)
}

func TestAPI_GetBillNotFound(t *testing.T) {
	svc := newTestService(t)

	_, err := svc.GetBill(context.Background(), "does-not-exist")
	require.Error(t, err)
	var errsErr *errs.Error
	require.ErrorAs(t, err, &errsErr)
	require.Equal(t, errs.NotFound, errsErr.Code)
}
