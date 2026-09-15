package bills

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "bills"

// Update validator errors are classified via ApplicationError.Type so the
// API layer (bills.go) can map them to the right HTTP status without
// parsing error strings.
const (
	ErrTypeInvalidInput  = "INVALID_INPUT"
	ErrTypeStateConflict = "STATE_CONFLICT"
)

type CreateBillInput struct {
	BillID    string
	AccountID string
	PeriodID  string
	Currency  Currency
}

type AddLineItemInput struct {
	IdempotencyKey string
	Description    string
	Amount         Money
}

type AddLineItemResult struct {
	LineItem     LineItem
	RunningTotal Money
}

func BillWorkflow(ctx workflow.Context, input CreateBillInput) (Bill, error) {
	if !input.Currency.Valid() {
		return Bill{}, temporal.NewApplicationError(fmt.Sprintf("invalid currency: %q", input.Currency), ErrTypeInvalidInput)
	}

	state := &Bill{
		ID:        input.BillID,
		AccountID: input.AccountID,
		PeriodID:  input.PeriodID,
		Currency:  input.Currency,
		Status:    StatusOpen,
		Total:     Money{AmountMinor: 0, Currency: input.Currency},
		CreatedAt: workflow.Now(ctx),
	}

	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    5,
		},
	})

	// AddLineItem and CloseBill both read-modify-write shared state across a
	// workflow.ExecuteActivity(...).Get() yield point. Update handlers run
	// as separate coroutines that can interleave at those yield points, so
	// without this mutex two concurrent updates can each read the
	// pre-update state, compute independently, and the second write clobbers
	// the first (a lost update). Every mutating handler below acquires it
	// for its entire body, including the activity call.
	mutex := workflow.NewMutex(ctx)

	seen := make(map[string]AddLineItemResult)
	var lineItemSeq int64

	addLineItem := func(ctx workflow.Context, in AddLineItemInput) (AddLineItemResult, error) {
		if err := mutex.Lock(ctx); err != nil {
			return AddLineItemResult{}, err
		}
		defer mutex.Unlock()

		// Re-checked here, not just in the validator: a concurrent
		// CloseBill may have completed while this handler was waiting on
		// the lock, and a concurrent AddLineItem with the same key may
		// have just recorded it.
		if result, ok := seen[in.IdempotencyKey]; ok {
			if result.LineItem.Description != in.Description || result.LineItem.Amount != in.Amount {
				return AddLineItemResult{}, temporal.NewApplicationError(
					"idempotency key already used with a different request", ErrTypeInvalidInput)
			}
			return result, nil
		}
		if state.Status != StatusOpen {
			return AddLineItemResult{}, temporal.NewApplicationError(
				fmt.Sprintf("cannot add line item: bill is %s", state.Status), ErrTypeStateConflict)
		}

		total, err := state.Total.Add(in.Amount)
		if err != nil {
			return AddLineItemResult{}, temporal.NewApplicationError(err.Error(), ErrTypeInvalidInput)
		}

		lineItemSeq++
		item := LineItem{
			ID:             fmt.Sprintf("%s-li-%d", state.ID, lineItemSeq),
			IdempotencyKey: in.IdempotencyKey,
			Description:    in.Description,
			Amount:         in.Amount,
			AddedAt:        workflow.Now(ctx),
		}

		err = workflow.ExecuteActivity(activityCtx, RecordLineItemActivity, RecordLineItemInput{
			BillID:         state.ID,
			LineItem:       item,
			RunningTotal:   total,
			SequenceNumber: lineItemSeq,
		}).Get(ctx, nil)
		if err != nil {
			lineItemSeq--
			return AddLineItemResult{}, err
		}

		state.LineItems = append(state.LineItems, item)
		state.Total = total

		result := AddLineItemResult{LineItem: item, RunningTotal: total}
		seen[in.IdempotencyKey] = result
		return result, nil
	}

	validateAddLineItem := func(in AddLineItemInput) error {
		if state.Status != StatusOpen {
			return temporal.NewApplicationError(fmt.Sprintf("cannot add line item: bill is %s", state.Status), ErrTypeStateConflict)
		}
		if in.IdempotencyKey == "" {
			return temporal.NewApplicationError("idempotency key is required", ErrTypeInvalidInput)
		}
		if in.Amount.Currency != state.Currency {
			return temporal.NewApplicationError(
				fmt.Sprintf("%s: bill is %s, line item is %s", ErrCurrencyMismatch, state.Currency, in.Amount.Currency),
				ErrTypeInvalidInput)
		}
		if err := in.Amount.Validate(); err != nil {
			return temporal.NewApplicationError(err.Error(), ErrTypeInvalidInput)
		}
		return nil
	}

	if err := workflow.SetUpdateHandlerWithOptions(ctx, "AddLineItem", addLineItem, workflow.UpdateHandlerOptions{
		Validator: validateAddLineItem,
	}); err != nil {
		return Bill{}, err
	}

	closeBill := func(ctx workflow.Context) (Bill, error) {
		if err := mutex.Lock(ctx); err != nil {
			return Bill{}, err
		}
		defer mutex.Unlock()

		if state.Status != StatusOpen {
			return Bill{}, temporal.NewApplicationError(fmt.Sprintf("cannot close bill: bill is %s", state.Status), ErrTypeStateConflict)
		}

		state.Status = StatusClosing
		closedAt := workflow.Now(ctx)

		err := workflow.ExecuteActivity(activityCtx, RecordBillClosedActivity, RecordBillClosedInput{
			BillID:         state.ID,
			Total:          state.Total,
			ClosedAt:       closedAt,
			SequenceNumber: lineItemSeq + 1,
		}).Get(ctx, nil)
		if err != nil {
			state.Status = StatusOpen
			return Bill{}, err
		}

		state.ClosedAt = &closedAt
		state.Status = StatusClosed
		return *state, nil
	}

	validateCloseBill := func() error {
		if state.Status != StatusOpen {
			return temporal.NewApplicationError(fmt.Sprintf("cannot close bill: bill is %s", state.Status), ErrTypeStateConflict)
		}
		return nil
	}

	if err := workflow.SetUpdateHandlerWithOptions(ctx, "CloseBill", closeBill, workflow.UpdateHandlerOptions{
		Validator: validateCloseBill,
	}); err != nil {
		return Bill{}, err
	}

	if err := workflow.SetQueryHandler(ctx, "GetBill", func() (Bill, error) {
		return *state, nil
	}); err != nil {
		return Bill{}, err
	}

	if err := workflow.Await(ctx, func() bool { return state.Status == StatusClosed }); err != nil {
		return Bill{}, err
	}

	return *state, nil
}
