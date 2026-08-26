CREATE TABLE bill_events (
    id BIGSERIAL PRIMARY KEY,
    bill_id TEXT NOT NULL REFERENCES bills (id),
    sequence_number BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    line_item_id TEXT REFERENCES line_items (id),
    running_total_minor BIGINT NOT NULL,
    currency TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,

    UNIQUE (bill_id, sequence_number)
);

CREATE INDEX line_items_bill_id_idx ON line_items (bill_id);
