package bills

import (
	"context"
	"time"
)

type RecordLineItemInput struct {
	BillID         string
	LineItem       LineItem
	RunningTotal   Money
	SequenceNumber int64
}

type RecordBillClosedInput struct {
	BillID         string
	Total          Money
	ClosedAt       time.Time
	SequenceNumber int64
}

// RecordLineItemActivity persists a line item and the bill's new running
// total. Every write uses the workflow-computed values (not a SQL
// increment), and every insert has an ON CONFLICT no-op on its natural key,
// so replaying this activity after an at-least-once retry is a no-op rather
// than a double-write.
func RecordLineItemActivity(ctx context.Context, in RecordLineItemInput) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec(ctx, `
		INSERT INTO line_items (id, bill_id, idempotency_key, description, amount_minor, currency, added_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (bill_id, idempotency_key) DO NOTHING
	`, in.LineItem.ID, in.BillID, in.LineItem.IdempotencyKey, in.LineItem.Description,
		in.LineItem.Amount.AmountMinor, in.LineItem.Amount.Currency, in.LineItem.AddedAt)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		UPDATE bills SET total_amount_minor = $2 WHERE id = $1
	`, in.BillID, in.RunningTotal.AmountMinor)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO bill_events (bill_id, sequence_number, event_type, line_item_id, running_total_minor, currency, occurred_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (bill_id, sequence_number) DO NOTHING
	`, in.BillID, in.SequenceNumber, EventLineItemAdded, in.LineItem.ID,
		in.RunningTotal.AmountMinor, in.RunningTotal.Currency, in.LineItem.AddedAt)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// RecordBillClosedActivity persists the bill's closed status and final
// total. Idempotent for the same reasons as RecordLineItemActivity.
func RecordBillClosedActivity(ctx context.Context, in RecordBillClosedInput) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec(ctx, `
		UPDATE bills SET status = $2, total_amount_minor = $3, closed_at = $4 WHERE id = $1
	`, in.BillID, StatusClosed, in.Total.AmountMinor, in.ClosedAt)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO bill_events (bill_id, sequence_number, event_type, line_item_id, running_total_minor, currency, occurred_at)
		VALUES ($1, $2, $3, NULL, $4, $5, $6)
		ON CONFLICT (bill_id, sequence_number) DO NOTHING
	`, in.BillID, in.SequenceNumber, EventBillClosed, in.Total.AmountMinor, in.Total.Currency, in.ClosedAt)
	if err != nil {
		return err
	}

	return tx.Commit()
}
