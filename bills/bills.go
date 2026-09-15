package bills

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"encore.dev/beta/errs"
	"encore.dev/storage/sqldb"
	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

type CreateBillRequest struct {
	AccountID string
	PeriodID  string
	Currency  Currency
}

//encore:api public method=POST path=/bills
func (s *Service) CreateBill(ctx context.Context, req *CreateBillRequest) (*Bill, error) {
	if req.AccountID == "" || req.PeriodID == "" {
		return nil, &errs.Error{Code: errs.InvalidArgument, Message: "account_id and period_id are required"}
	}
	if !req.Currency.Valid() {
		return nil, &errs.Error{Code: errs.InvalidArgument, Message: fmt.Sprintf("invalid currency: %q", req.Currency)}
	}

	billID := fmt.Sprintf("%s-%s", req.AccountID, req.PeriodID)

	_, err := s.temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       billID,
		TaskQueue:                TaskQueue,
		WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		// FAILED_ONLY (not REJECT_DUPLICATE): a bill that completed
		// successfully (closed) can never be recreated, but a bill whose
		// workflow was compensated away below (terminated because its
		// projection row failed to insert) must be retryable, or a
		// transient Postgres error would permanently strand that
		// account+period.
		WorkflowIDReusePolicy:                    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
	}, BillWorkflow, CreateBillInput{
		BillID:    billID,
		AccountID: req.AccountID,
		PeriodID:  req.PeriodID,
		Currency:  req.Currency,
	})
	if err != nil {
		var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
		if errors.As(err, &alreadyStarted) {
			return nil, &errs.Error{Code: errs.AlreadyExists, Message: "a bill already exists for this account and period"}
		}
		return nil, errs.WrapCode(err, errs.Internal, "failed to start bill workflow")
	}

	createdAt := time.Now()
	_, err = db.Exec(ctx, `
		INSERT INTO bills (id, account_id, period_id, currency, status, total_amount_minor, created_at)
		VALUES ($1, $2, $3, $4, $5, 0, $6)
	`, billID, req.AccountID, req.PeriodID, req.Currency, StatusOpen, createdAt)
	if err != nil {
		// Compensate: without this, the workflow would keep running with
		// no matching Postgres row, and — since AddLineItem/CloseBill
		// activities FK-reference bills.id — every future call against
		// this bill ID would fail permanently.
		_ = s.temporal.TerminateWorkflow(ctx, billID, "", "compensating: bill projection failed to persist")
		return nil, errs.WrapCode(err, errs.Internal, "failed to persist bill; please retry")
	}

	return &Bill{
		ID:        billID,
		AccountID: req.AccountID,
		PeriodID:  req.PeriodID,
		Currency:  req.Currency,
		Status:    StatusOpen,
		Total:     Money{AmountMinor: 0, Currency: req.Currency},
		CreatedAt: createdAt,
	}, nil
}

type AddLineItemRequest struct {
	IdempotencyKey string
	Description    string
	Amount         Money
}

//encore:api public method=POST path=/bills/:id/line-items
func (s *Service) AddLineItem(ctx context.Context, id string, req *AddLineItemRequest) (*AddLineItemResult, error) {
	if req.IdempotencyKey == "" {
		return nil, &errs.Error{Code: errs.InvalidArgument, Message: "idempotency_key is required"}
	}

	var result AddLineItemResult
	input := AddLineItemInput{
		IdempotencyKey: req.IdempotencyKey,
		Description:    req.Description,
		Amount:         req.Amount,
	}
	if err := s.sendUpdate(ctx, id, "AddLineItem", []interface{}{input}, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

//encore:api public method=POST path=/bills/:id/close
func (s *Service) CloseBill(ctx context.Context, id string) (*Bill, error) {
	var result Bill
	if err := s.sendUpdate(ctx, id, "CloseBill", nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// sendUpdate sends a Temporal Update to the bill's workflow and blocks
// until it completes, translating Temporal-level and workflow-validator
// errors into the right HTTP status.
func (s *Service) sendUpdate(ctx context.Context, billID, updateName string, args []interface{}, result interface{}) error {
	handle, err := s.temporal.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   billID,
		UpdateName:   updateName,
		Args:         args,
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return classifyUpdateError(ctx, billID, err)
	}
	if err := handle.Get(ctx, result); err != nil {
		return classifyUpdateError(ctx, billID, err)
	}
	return nil
}

// classifyUpdateError translates Temporal-level and workflow-validator
// errors into the right HTTP status. The tricky case: the workflow
// completes the instant CloseBill succeeds, so a later update against it
// doesn't reach our validator at all — Temporal rejects it server-side
// with the same serviceerror.NotFound it would return for a bill ID that
// never existed. We disambiguate the two using our own Postgres
// projection rather than guessing from the error shape.
func classifyUpdateError(ctx context.Context, billID string, err error) error {
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) {
		switch appErr.Type() {
		case ErrTypeStateConflict:
			return &errs.Error{Code: errs.Aborted, Message: appErr.Message()}
		case ErrTypeInvalidInput:
			return &errs.Error{Code: errs.InvalidArgument, Message: appErr.Message()}
		}
	}

	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) || strings.Contains(err.Error(), "workflow execution already completed") {
		var status BillStatus
		lookupErr := db.QueryRow(ctx, `SELECT status FROM bills WHERE id = $1`, billID).Scan(&status)
		switch {
		case lookupErr == nil && status == StatusClosed:
			return &errs.Error{Code: errs.Aborted, Message: "bill is closed"}
		case errors.Is(lookupErr, sqldb.ErrNoRows):
			return &errs.Error{Code: errs.NotFound, Message: "bill not found"}
		case lookupErr != nil:
			return errs.WrapCode(lookupErr, errs.Internal, "failed to look up bill while classifying update error")
		default:
			// Row exists but isn't CLOSED (e.g. CLOSING) — Temporal still
			// rejected the update, so surface it as a conflict rather than
			// a false 404.
			return &errs.Error{Code: errs.Aborted, Message: "bill update rejected"}
		}
	}

	return errs.WrapCode(err, errs.Internal, "workflow update failed")
}

//encore:api public method=GET path=/bills/:id
func (s *Service) GetBill(ctx context.Context, id string) (*Bill, error) {
	bill := &Bill{ID: id}
	var closedAt *time.Time
	err := db.QueryRow(ctx, `
		SELECT account_id, period_id, currency, status, total_amount_minor, created_at, closed_at
		FROM bills WHERE id = $1
	`, id).Scan(&bill.AccountID, &bill.PeriodID, &bill.Currency, &bill.Status, &bill.Total.AmountMinor, &bill.CreatedAt, &closedAt)
	if err != nil {
		if errors.Is(err, sqldb.ErrNoRows) {
			return nil, &errs.Error{Code: errs.NotFound, Message: "bill not found"}
		}
		return nil, err
	}
	bill.Total.Currency = bill.Currency
	bill.ClosedAt = closedAt

	rows, err := db.Query(ctx, `
		SELECT id, idempotency_key, description, amount_minor, currency, added_at
		FROM line_items WHERE bill_id = $1 ORDER BY added_at
	`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var li LineItem
		if err := rows.Scan(&li.ID, &li.IdempotencyKey, &li.Description, &li.Amount.AmountMinor, &li.Amount.Currency, &li.AddedAt); err != nil {
			return nil, err
		}
		bill.LineItems = append(bill.LineItems, li)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return bill, nil
}

type ListBillEventsResponse struct {
	Events []BillEvent
}

//encore:api public method=GET path=/bills/:id/events
func (s *Service) ListBillEvents(ctx context.Context, id string) (*ListBillEventsResponse, error) {
	rows, err := db.Query(ctx, `
		SELECT sequence_number, event_type, COALESCE(line_item_id, ''), running_total_minor, currency, occurred_at
		FROM bill_events WHERE bill_id = $1 ORDER BY sequence_number
	`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := []BillEvent{}
	for rows.Next() {
		e := BillEvent{BillID: id}
		if err := rows.Scan(&e.SequenceNumber, &e.Type, &e.LineItemID, &e.RunningTotal.AmountMinor, &e.RunningTotal.Currency, &e.OccurredAt); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &ListBillEventsResponse{Events: events}, nil
}
