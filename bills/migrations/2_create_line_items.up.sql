CREATE TABLE line_items (
    id TEXT PRIMARY KEY,
    bill_id TEXT NOT NULL REFERENCES bills (id),
    idempotency_key TEXT NOT NULL,
    description TEXT NOT NULL,
    amount_minor BIGINT NOT NULL,
    currency TEXT NOT NULL,
    added_at TIMESTAMPTZ NOT NULL,

    UNIQUE (bill_id, idempotency_key)
);
