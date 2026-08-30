# EventHub

A multi-tenant event and community platform, built as four services over Postgres
and Kafka.

It exists to be **explainable**. Every non-obvious decision in this repository has
a comment saying what it is working around, and most of them are working around
something specific — a Postgres behaviour, a Kafka guarantee, or a failure that
was observed rather than imagined. Where the code claims a property, there is
usually a script that demonstrates it and a CI step that re-checks it on every
pull request.

## The problems it actually solves

Anyone can wire up CRUD. These are the parts that took thought.

**Two people buy the last seat at the same time.** The naive read-then-write has
no protection under `READ COMMITTED` — both transactions read `remaining = 1`,
both conclude they may proceed, and the seat sells twice. Wrapping it in a
transaction changes nothing, because transactions give atomicity, not mutual
exclusion. The fix is to make the check and the write *one statement*, so the
second transaction blocks on the row lock and then re-evaluates its `WHERE`
clause against the newest committed row. CI fires 40 concurrent buyers at 5 seats
and asserts exactly 5 succeed.

**The payment provider doesn't answer.** Not fails — *fails to answer*. The
charge may have succeeded and the reply may have been lost, and nothing on this
side can tell. So `UNKNOWN` is a first-class state, distinct from `FAILED`:
rounding "I don't know" down to "no" releases seats that were genuinely paid for.
An unanswered charge keeps its seats held and stays unsettled, and a reconciler
resolves it later by **asking the provider** rather than guessing. There are
exactly two wrong ways to resolve a stuck charge and one right one.

**The money and the seats live in different systems.** There is no transaction
spanning both, so the payment endpoint is three phases with a commit between
each. One transaction wrapped around the provider call is wrong twice over: it
holds a row lock across a network call, and it's a lie — the database can roll
back, the charge cannot.

**A webhook arrives with no tenant.** Every row-security policy here reads
`app.tenant_id`, so the application role sees *zero rows* for a payment that
plainly exists. The tempting fix is to read the tenant off the signature-verified
payload — but authenticity is not authority; that would let the request choose
whose rows it modifies. The webhook says *which payment*; the database says
*whose*. The cross-tenant reach is one `SELECT` of one column.

**One service can't join to another's tables.** Database-per-service with
`CONNECT` revoked means Ticketing genuinely cannot read Event's data. So it
consumes an event stream and maintains a projection instead. That is the honest
price of the architecture: a join becomes a subscription, and you inherit
at-least-once delivery, out-of-order arrival, and the need to dedupe.

## Tenant isolation is enforced by Postgres, not by application code

Row-level security with `FORCE`, role-targeted policies, and a per-transaction
session variable. Three details make it real rather than decorative:

- A table owner **silently bypasses its own policies**, so services connect as a
  separate non-owner role. Migrations run as the owner and therefore see zero
  rows under `FORCE` — which has bitten this repo three times during backfills,
  each one documented in the migration that hit it.
- Foreign keys, unique constraints and cascading deletes all run **outside** row
  security. So every child references a `(tenantId, id)` pair rather than a bare
  id, and no unique index omits `tenantId` — otherwise a `23505` would reveal that
  another tenant already used a value.
- Grants are explicit per table, with no `ALTER DEFAULT PRIVILEGES`. A forgotten
  policy is therefore also a forgotten grant, and the service fails loudly instead
  of quietly serving rows across every tenant. **Fail closed.**

CI asserts that no table anywhere is missing RLS unless it is on a documented
exemption list — and each exemption has to be justified in its migration, because
an exemption nobody wrote down is indistinguishable from a policy somebody forgot.

## What's here

| | |
|---|---|
| `services/auth` | Invite-only registration, argon2id, RS256 JWTs, refresh rotation with family reuse detection, password reset |
| `services/event` | Events CRUD with a transactional outbox |
| `services/relay` | Drains an outbox, Avro-encodes, publishes to Kafka, dead-letters poison rows |
| `services/ticketing` | Projection consumer, seat reservation, hold sweeper, payment path, payment reconciler, provider webhook |
| `contracts/` | Avro schemas, and TypeScript types generated from them |
| `db/init/` | Three databases and nine roles, with `CONNECT` revoked from `PUBLIC` |
| `scripts/verify-*.mjs` | 13 executable proofs of the invariants above |

Three separate Postgres databases with their own credentials. CI asserts across
all six directions that no service can even open a connection to another's.

## Running it

Node is pinned in `.nvmrc` and CI reads the version from that same file, so there
is one place to change it. `pnpm` is pinned via `packageManager`.

```bash
pnpm install && pnpm build
```

Bring up Postgres, Kafka and Schema Registry. `--wait` blocks until every
healthcheck passes, so nothing below races a half-started broker:

```bash
docker compose up -d --wait
```

The Postgres container creates all three databases and their roles on first boot
through its own `/docker-entrypoint-initdb.d` hook, so there is no separate
bootstrap step.

Each service ships a committed `.env.example`; copy it to `.env` (the real one is
gitignored). Each carries **two** connection strings on purpose — `DATABASE_URL`
owns the tables and runs migrations, `APP_DATABASE_URL` owns nothing and is what
the service connects as at runtime. That separation is what makes row-level
security work at all.

Then migrate each service and seed:

```bash
pnpm --filter @eventhub/auth exec prisma migrate deploy
```

```bash
pnpm --filter @eventhub/auth exec prisma db seed
```

Finally, a throwaway signing keypair:

```bash
node services/auth/scripts/generate-keys.mjs
```

Keys are gitignored. Never reuse a real signing key here — anything in a build
environment is readable by anything else running there.

**`.github/workflows/ci.yml` is the authoritative setup.** It runs the same
`docker-compose.yml` a laptop does, from the same file with the same config, so
the two cannot drift. If anything above disagrees with it, believe the workflow.

## On the verification scripts

They aren't unit tests. Each one demonstrates a property that is invisible from
outside the system and that the schema cannot express:

- seats cannot be oversold under real concurrency
- a price cannot move under a buyer mid-checkout
- two sweepers return each seat exactly once
- an unanswered charge leaves its seats held
- reconciliation resolves both directions rather than guessing
- a refresh token can be spent only once, **even under a race**
- a password reset actually changes the password

That last pair matter because their failure modes are silent. A refresh token
double-spend leaves reuse detection *blind* rather than bypassed — each racer
walks away with a distinct successor, so no token is ever presented twice and
the detector has nothing to see. A password reset written without tenant context
matches zero rows, which a guarded `updateMany` reports as a count of 0 rather
than an error: the endpoint returns 200, the token is spent, and the password
never changed.

Every assertion here has been watched fail against deliberately broken code
before being trusted. An assertion nobody has seen go red is a guess.

## Status

Chapters 1–4 are built: foundations, auth and RLS, the Event service and the
Kafka backbone, and Ticketing through payments. Later chapters (live features,
search, analytics, notifications, community) are designed and documented but not
implemented.

Known gaps, named rather than hidden:

- No API gateway, so rate limiting exists only inside the Auth service and only
  in process memory.
- Access tokens are 15-minute stateless RS256 with no revocation list, so a
  password reset kills refresh tokens immediately but cannot evict an access
  token already issued.
- Auth has no outbox, so invite and reset links reach a developer through an
  opt-in sink that refuses to run in production, rather than by email.
