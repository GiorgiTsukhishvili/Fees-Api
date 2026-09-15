package bills

import (
	"context"
	"testing"

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

	bill, err := svc.CreateBill(ctx, &CreateBillRequest{
		AccountID: "api-test-acct",
		PeriodID:  "2026-11",
		Currency:  USD,
	})
	require.NoError(t, err)
	require.Equal(t, StatusOpen, bill.Status)

	_, err = svc.CreateBill(ctx, &CreateBillRequest{
		AccountID: "api-test-acct",
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
