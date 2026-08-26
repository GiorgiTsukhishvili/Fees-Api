package bills

import (
	"errors"
	"fmt"

	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "bills"

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
		return Bill{}, fmt.Errorf("%w: %q", ErrInvalidCurrency, input.Currency)
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

	seen := make(map[string]AddLineItemResult)
	var lineItemSeq int64

	addLineItem := func(ctx workflow.Context, in AddLineItemInput) (AddLineItemResult, error) {
		if result, ok := seen[in.IdempotencyKey]; ok {
			return result, nil
		}

		total, err := state.Total.Add(in.Amount)
		if err != nil {
			return AddLineItemResult{}, err
		}

		lineItemSeq++
		item := LineItem{
			ID:             fmt.Sprintf("%s-li-%d", state.ID, lineItemSeq),
			IdempotencyKey: in.IdempotencyKey,
			Description:    in.Description,
			Amount:         in.Amount,
			AddedAt:        workflow.Now(ctx),
		}
		state.LineItems = append(state.LineItems, item)
		state.Total = total

		result := AddLineItemResult{LineItem: item, RunningTotal: total}
		seen[in.IdempotencyKey] = result
		return result, nil
	}

	validateAddLineItem := func(in AddLineItemInput) error {
		if state.Status != StatusOpen {
			return fmt.Errorf("cannot add line item: bill is %s", state.Status)
		}
		if in.IdempotencyKey == "" {
			return errors.New("idempotency key is required")
		}
		if in.Amount.Currency != state.Currency {
			return fmt.Errorf("%w: bill is %s, line item is %s", ErrCurrencyMismatch, state.Currency, in.Amount.Currency)
		}
		return in.Amount.Validate()
	}

	if err := workflow.SetUpdateHandlerWithOptions(ctx, "AddLineItem", addLineItem, workflow.UpdateHandlerOptions{
		Validator: validateAddLineItem,
	}); err != nil {
		return Bill{}, err
	}

	closeBill := func(ctx workflow.Context) (Bill, error) {
		state.Status = StatusClosing
		closedAt := workflow.Now(ctx)
		state.ClosedAt = &closedAt
		state.Status = StatusClosed
		return *state, nil
	}

	validateCloseBill := func() error {
		if state.Status != StatusOpen {
			return fmt.Errorf("cannot close bill: bill is %s", state.Status)
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
