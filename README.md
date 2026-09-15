# Fees API

A Fees API built on [Encore](https://encore.dev) and [Temporal](https://temporal.io): a bill is opened at the start of a
billing period, accrues line items progressively, and is closed with a final invoice total.

## Architecture

```
HTTP request → Encore API endpoint (bills.go)
                      │
                      ▼
            Temporal client call (start / update / query)
                      │
                      ▼
   Temporal server ── durable history ── dispatches to ──▶ Worker (one per bill's lifetime)
                      │                                           │
                      ▼                                           ▼
              bill_events / bills / line_items  ◀── Activities ── BillWorkflow (workflow.go)
                 (Postgres, read projection)      (transactional writes)
```

One Temporal workflow execution = one bill's entire lifecycle, from `CreateBill` to `CloseBill`. The workflow
(`BillWorkflow`) holds the bill's live state — status, line items, running total — entirely in memory, mutated only
through two Temporal **Update** handlers (`AddLineItem`, `CloseBill`) and inspectable through one **Query** handler
(`GetBill`). Postgres is deliberately *not* where in-flight correctness lives: it's a read-optimized projection,
written by Activities as a side effect of workflow decisions, used only so `GET` requests are fast and don't need to
talk to Temporal. If workflow history and the Postgres projection ever disagree, the workflow history is correct —
Postgres is a cache of it.

## Why Temporal

A billing period can run for days or weeks, accruing fees from unrelated callers at any time, and the system has to
guarantee three things a plain CRUD service makes hard to get right simultaneously: **no lost or double-counted fee**
under concurrent writes or retries, **no charge accepted after close**, and **survival of a crash mid-accrual** without
manual reconciliation.

Temporal gives us all three from primitives rather than hand-rolled machinery:

- **Race-free state mutation.** Every `AddLineItem` and `CloseBill` call is a Temporal Update — the workflow processes
  updates one at a time, in the order the server sequences them, so two concurrent fee accruals can never
  interleave and corrupt the running total. No row locks, no optimistic-concurrency retry loops.
- **Synchronous, pre-mutation rejection.** Each Update has a validator that runs *before* the update is admitted into
  workflow history. A line item on a closed bill, or in the wrong currency, is rejected as a normal synchronous RPC
  failure — the caller doesn't have to poll to find out their request didn't apply.
- **Crash-safe durability without an outbox table.** The workflow's entire decision history — every line item
  accepted, every status transition — is Temporal's durable event log. If the worker process dies mid-accrual, a new
  worker replays that history and resumes exactly where execution left off; nothing is lost, nothing double-applies.
- **At-least-once I/O made safe, not avoided.** The actual Postgres writes happen in Activities, which Temporal
  retries independently (exponential backoff, capped attempts) of the workflow's own determinism constraints. Each
  activity is written to be idempotent under retry (see below), which is what makes "retry until it works" a safe
  default instead of a source of duplicate charges.

## Money and currency

- **No `float64`, anywhere.** `Money` is an integer amount in the currency's minor unit (cents, tetri) plus a
  `Currency`. Binary floating point cannot represent decimal currency amounts exactly, and that error compounds
  across many small accruals — unacceptable for a ledger.
- **`Currency` is a closed enum** (`GEL`, `USD` only), not a free string, so an invalid currency can't silently
  flow through the system.
- **`Money.Add` refuses to sum different currencies.** This one function is the guard that prevents GEL and USD
  ever being added into a meaningless total.
- **A bill is single-currency by design.** All line items on a bill share the bill's currency. An account accruing
  fees in both GEL and USD in the same period gets two bills (two workflow executions), not one bill with a mixed
  total — that keeps `Total` a single `Money` value instead of a currency-keyed map, and keeps every sum
  trivially correct.
- **Line items only accrue charges, not credits.** `Money.Validate` rejects zero or negative amounts; a refund or
  adjustment would be a distinct, explicitly-signed operation, not a "negative fee."

## State machine

`OPEN → CLOSING → CLOSED`. `CLOSING` is a short-lived transitional state entered the instant `CloseBill` is
accepted, before the close is durably persisted to Postgres. It exists for crash-safety: if the persistence Activity
fails after retries are exhausted, the workflow reverts back to `OPEN` (rather than being stuck in `CLOSING` forever
with no way to retry) — see `closeBill` in `bills/workflow.go`.

## Idempotency and retries

- **`AddLineItem` is idempotent per caller-supplied key**, both in the workflow (an in-memory map, safe because of
  Temporal's replay determinism) and at the database layer (`UNIQUE (bill_id, idempotency_key)` on `line_items`,
  with `ON CONFLICT DO NOTHING`) — two independent layers, so a retried request never double-charges even if one
  layer is bypassed.
- **Activities write absolute values, not increments** (`SET total_amount_minor = $running_total`, not `+= $amount`),
  computed once, deterministically, by the workflow. That makes retrying an activity a no-op rather than a
  double-application, regardless of *why* it's being retried (crash, at-least-once redelivery, timeout).
- **`bill_events`** is an append-only audit ledger — every status transition and every line item addition, with the
  running total as it stood right after — giving a full audit trail beyond the current-state snapshot in `bills`.
  `line_items` itself is also immutable/append-only, so it doubles as line-item history.

## Error mapping

HTTP status codes follow Encore's `errs` package:

| Situation | Code |
|---|---|
| Missing/invalid request fields, cross-currency line item, non-positive amount | `400 invalid_argument` |
| Bill not found | `404 not_found` |
| Duplicate `CreateBill` for the same account+period | `409 already_exists` |
| Line item or close attempted on a closed bill | `409 aborted` |

One non-obvious wrinkle, worth calling out because it cost real debugging time: the workflow *completes* the instant
`CloseBill` succeeds (its `Await` unblocks and the function returns). A *second* update sent after that point never
reaches our validator at all — Temporal's server rejects it directly with `serviceerror.NotFound`, the same error
shape it returns for a bill ID that never existed. The API layer disambiguates the two by cross-checking our own
Postgres projection (`classifyUpdateError` in `bills.go`) rather than guessing from the error shape.

## Running it

Prerequisites: [Encore CLI](https://encore.dev/docs/install), Go 1.22+, [Temporal CLI](https://docs.temporal.io/cli#install),
Docker (Encore uses it to run local Postgres).

```bash
# terminal 1 — local Temporal server + UI (http://localhost:8233)
temporal server start-dev

# terminal 2 — the app (auto-provisions Postgres, applies migrations, starts the worker)
encore run
```

The API is now at `http://localhost:4000`.

## Testing

```bash
encore test ./...
```

Must use `encore test`, not plain `go test` — Encore's `sqldb` package requires the Encore runtime to be present
(it panics with a clear message if run outside it), which the `encore test` wrapper provides. This runs:

- `bills/money_test.go` — table-driven unit tests for `Money.Validate`/`Add`.
- `bills/workflow_test.go` — the full bill lifecycle against Temporal's `testsuite` package (a deterministic
  simulated clock, no real server needed, activities mocked): accrual, idempotent duplicate handling, cross-currency
  rejection, close, and rejection of a line item on a closed bill, all in one run.
- `bills/bills_test.go` — API-level tests calling the service methods directly against a real local Temporal
  connection (`encore test` needs `temporal server start-dev` running, same as `encore run`).

## Example lifecycle (curl)

```bash
# Create a bill for an account/period
curl -X POST localhost:4000/bills \
  -d '{"AccountID":"acct-42","PeriodID":"2026-09","Currency":"USD"}'
# → { "ID": "acct-42-2026-09", "Status": "OPEN", "Total": {"AmountMinor": 0, "Currency": "USD"}, ... }

# Accrue a fee (idempotency key required — safe to retry with the same key)
curl -X POST localhost:4000/bills/acct-42-2026-09/line-items \
  -d '{"IdempotencyKey":"k1","Description":"monthly maintenance","Amount":{"AmountMinor":1500,"Currency":"USD"}}'

curl -X POST localhost:4000/bills/acct-42-2026-09/line-items \
  -d '{"IdempotencyKey":"k2","Description":"wire fee","Amount":{"AmountMinor":2500,"Currency":"USD"}}'

# Wrong currency is rejected synchronously (400)
curl -X POST localhost:4000/bills/acct-42-2026-09/line-items \
  -d '{"IdempotencyKey":"k3","Description":"bad currency","Amount":{"AmountMinor":100,"Currency":"GEL"}}'

# Close the bill — returns the final total and every line item
curl -X POST localhost:4000/bills/acct-42-2026-09/close

# A line item on a closed bill is rejected (409)
curl -X POST localhost:4000/bills/acct-42-2026-09/line-items \
  -d '{"IdempotencyKey":"k4","Description":"too late","Amount":{"AmountMinor":100,"Currency":"USD"}}'

# Read the projection (Postgres only, no Temporal round-trip)
curl localhost:4000/bills/acct-42-2026-09

# Full audit history: every accrual and transition, with running totals
curl localhost:4000/bills/acct-42-2026-09/events
```

The Temporal Web UI (`http://localhost:8233`) shows each bill as one workflow execution — `Running` while `OPEN`,
`Completed` the instant it closes.

## AI-assisted development

This solution was built iteratively with Claude (Sonnet 5) as a pair-programming collaborator, then given a critical
code review pass focused on `workflow.go` and `bills.go` — the review's findings (and the fixes applied) are in the
git history rather than repeated here.
