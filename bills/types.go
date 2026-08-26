package bills

import (
	"errors"
	"fmt"
	"time"
)

type Currency string

const (
	GEL Currency = "GEL"
	USD Currency = "USD"
)

func (c Currency) Valid() bool {
	switch c {
	case GEL, USD:
		return true
	default:
		return false
	}
}

var (
	ErrInvalidCurrency   = errors.New("invalid currency")
	ErrNonPositiveAmount = errors.New("amount must be positive")
	ErrCurrencyMismatch  = errors.New("currency mismatch")
)

// Amounts are always integer minor units (cents, tetri) — never float64.
type Money struct {
	AmountMinor int64
	Currency    Currency
}

func (m Money) Validate() error {
	if !m.Currency.Valid() {
		return fmt.Errorf("%w: %q", ErrInvalidCurrency, m.Currency)
	}
	if m.AmountMinor <= 0 {
		return fmt.Errorf("%w: %d", ErrNonPositiveAmount, m.AmountMinor)
	}
	return nil
}

func (m Money) Add(other Money) (Money, error) {
	if m.Currency != other.Currency {
		return Money{}, fmt.Errorf("%w: %s vs %s", ErrCurrencyMismatch, m.Currency, other.Currency)
	}
	return Money{AmountMinor: m.AmountMinor + other.AmountMinor, Currency: m.Currency}, nil
}

type BillStatus string

const (
	StatusOpen BillStatus = "OPEN"
	// Transitional: set before the close is durably persisted so a crash
	// mid-close still rejects new line items on replay.
	StatusClosing BillStatus = "CLOSING"
	StatusClosed  BillStatus = "CLOSED"
)

type LineItem struct {
	ID             string
	IdempotencyKey string
	Description    string
	Amount         Money
	AddedAt        time.Time
}

// A bill is single-currency; an account accruing fees in both GEL and USD
// in one period gets two bills, not one mixed bill.
type Bill struct {
	ID        string
	AccountID string
	PeriodID  string
	Currency  Currency
	Status    BillStatus
	LineItems []LineItem
	Total     Money
	CreatedAt time.Time
	ClosedAt  *time.Time
}

type BillEventType string

const (
	EventBillCreated   BillEventType = "BILL_CREATED"
	EventLineItemAdded BillEventType = "LINE_ITEM_ADDED"
	EventBillClosing   BillEventType = "BILL_CLOSING"
	EventBillClosed    BillEventType = "BILL_CLOSED"
)

// BillEvent is one entry in a bill's append-only audit log: every status
// transition and every line item addition, in order, with the running
// total as it stood right after the event. LineItemID is set only for
// EventLineItemAdded.
type BillEvent struct {
	BillID         string
	SequenceNumber int64
	Type           BillEventType
	LineItemID     string
	RunningTotal   Money
	OccurredAt     time.Time
}
