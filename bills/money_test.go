package bills

import (
	"errors"
	"testing"
)

func TestMoney_Validate(t *testing.T) {
	tests := []struct {
		name    string
		money   Money
		wantErr error
	}{
		{"valid USD", Money{AmountMinor: 100, Currency: USD}, nil},
		{"valid GEL", Money{AmountMinor: 1, Currency: GEL}, nil},
		{"unknown currency", Money{AmountMinor: 100, Currency: "EUR"}, ErrInvalidCurrency},
		{"zero amount", Money{AmountMinor: 0, Currency: USD}, ErrNonPositiveAmount},
		{"negative amount", Money{AmountMinor: -50, Currency: USD}, ErrNonPositiveAmount},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.money.Validate()
			if tt.wantErr == nil {
				if err != nil {
					t.Errorf("Validate() = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Validate() = %v, want error wrapping %v", err, tt.wantErr)
			}
		})
	}
}

func TestMoney_Add(t *testing.T) {
	sum, err := Money{AmountMinor: 100, Currency: USD}.Add(Money{AmountMinor: 50, Currency: USD})
	if err != nil {
		t.Fatalf("Add() error = %v, want nil", err)
	}
	if sum != (Money{AmountMinor: 150, Currency: USD}) {
		t.Errorf("Add() = %+v, want {150 USD}", sum)
	}

	_, err = Money{AmountMinor: 100, Currency: USD}.Add(Money{AmountMinor: 50, Currency: GEL})
	if !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Add() across currencies = %v, want error wrapping ErrCurrencyMismatch", err)
	}
}
