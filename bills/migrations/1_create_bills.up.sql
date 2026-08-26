CREATE TABLE bills (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    period_id TEXT NOT NULL,
    currency TEXT NOT NULL,
    status TEXT NOT NULL,
    total_amount_minor BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    closed_at TIMESTAMPTZ,

    UNIQUE (account_id, period_id)
);
