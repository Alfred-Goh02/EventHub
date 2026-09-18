# Working rules for this repository

Every rule below exists because of a specific mistake made in this repository,
and each one is followed by what that mistake cost. They are written down
because the same mistakes kept recurring across sessions, and a rule with its
evidence attached is harder to rationalise away than a rule on its own.

## Never edit a file by manipulating its text in the shell

Use the Edit or Write tool. Not `sed -i`, not a heredoc'd patch script, not
`node -e` with a regex replace.

What this cost, all in this repository:

- A quoted heredoc ate the backslash in `'\\'` and wrote `'\'` instead.
- An unquoted one ran `` `tsx watch` `` as a command substitution, because
  backticks inside double quotes are substitution, not literal text.
- A patch script matched nothing, changed nothing, and printed
  `patched successfully`.

Every one of those looked like a working change and took a round trip to
disprove. The shell is the wrong tool for structured edits because its failure
mode is silent and plausible.

If a script genuinely must perform one mechanical edit across many files, it
asserts its own effect: count the matches it made, and exit non-zero when the
count is zero. A patch that can no-op silently is worse than no patch at all,
because it produces a green log and an unchanged file.

## Never trust an assertion you have not watched fail

Before an assertion counts as an assertion, break the thing it guards and
watch it go red. If it cannot be made to fail, it is not testing anything.

What this cost, all real, all found here:

- `seen.sort()` sorts **in place**. A display loop sorted the array, and the
  ordering check then read the same array and could never fail. Fed `[3,1,2]`
  it reported `monotonic: true`.
- `check(relayB.length >= 0 && bTookOurs.length === 0, ...)` printed
  `0 message(s)` and called it a pass. Both halves are true of nothing, so the
  assertion held most strongly in the case it was meant to rule out.

Hence the positive controls throughout `scripts/verify-*.mjs`: a test that
asserts a property must also assert that the machinery producing the property
is actually running. A suite where every path collapsed to zero would
otherwise agree with itself and report success.

A test that has never been red is documentation that looks like a test.

## Qualify every ORDER BY column

Postgres resolves a bare name in `ORDER BY` against the **output** columns
first. So in a query selecting `seq::text AS "seq"`, `ORDER BY seq` sorts the
text — `'10'` before `'9'`.

This was introduced here while *fixing* an ordering bug (#64). It silently
reordered outbox claims, which corrupts projection rebuilds on the compacted
topic, and no test aimed at it caught it — #63's unrelated rebuild check did.

Write `ORDER BY c.seq`. Never `ORDER BY seq`.

## Scope every assertion to data this run created

A verifier once failed its own positive control on rows a previous run had left
in the outbox. Generate an aggregate id per run and scope every query to it
(the `of(id)` helper in `scripts/verify-relay-ordering.mjs`), and where a table
is supposed to be empty at the start, refuse to run when it is not rather than
asserting against whatever is there.

## A test must not leave the database worse than it found it

A regression test derived its fixture sequence from the current maximum and
advanced the outbox sequence to the next power of ten on every run — ten times
per run, which exhausts a bigint in about sixteen local runs. Use explicit
fixture values. Never derive one from current state.

## Branch, PR, CI, squash — in that order

Check the branch **before** the first commit, not after:
`git rev-parse --abbrev-ref HEAD`. If it prints `main`, stop and branch.

Never commit to main, never merge into main locally, never force-push it. Push
the branch, open a PR, wait for CI to go green, then squash-merge. Remove a
worktree with `git worktree remove` before deleting its branch, or the delete
fails with the worktree still holding it.

## Commits carry Alfred's name only

No `Co-Authored-By` trailer on commits. No generated-with line in PR
descriptions.

## This is Windows

PowerShell or Git Bash. `core.autocrlf` is true, which has two consequences
that have both bitten: a regex anchored to `$` must tolerate CRLF, and a file
fetched for byte-exact comparison is fetched raw — never through `git clone`,
which rewrites line endings on the way in.

Git Bash also rewrites an absolute path handed to `docker exec`: `/app/...` arrived
as `C:/Program Files/Git/app/...`, and the command failed as if the file were
missing. Prefix `MSYS_NO_PATHCONV=1`.

## A guard that runs after the thing it guards is not a guard

Check the precondition **before** the call that depends on it, not after.

`verify-topic-configs.mjs` grew a guard against an empty `EXPECTED` list, and it
was placed below the `describeConfigs` call. With `EXPECTED` empty the topic list
is empty too, so the broker call threw `Resources array cannot be empty` first and
the new error handler blamed a missing topic — a confident, wrong diagnosis, from
the very check added to prevent one. Caught only by running it with the list
emptied.

## Enumerate the category, then ask what nothing covers

The highest-yield way to find real bugs in this repository has not been reading
code hoping to notice something. It is picking a category, listing **every**
member mechanically, and asking which members nothing covers:

- every HTTP route → `/logout` had no test at all
- every exported function → a second, weaker copy of token verification that
  nothing called
- every environment variable the code reads → seventeen undocumented knobs
- every key a redaction list claims → three that leaked
- everything inside the built image → a real `.env` with database credentials

Clean sweeps are worth the same trouble and cost a minute: verify scripts versus
CI, cited file paths, and database roles all came back with nothing, which is
itself a fact worth having.

## A detector that reads comments finds uses that are not uses

Strip comments before scanning source for anything.

The environment-variable audit reported that the relay reads `PORT` and
`DATABASE_URL`. It does not — it deliberately does not call `loadBaseEnv`, and
says so in a comment that names the function twice while explaining why. The
scan read the explanation as a use, and would have had me document two variables
the relay has never heard of. Match the **call**, not the name.

## An ignore rule that names one file misses the family

`**/.env` matches a file called `.env` and nothing else. This repository also has
`.env.ticketing`, so the rule that exists to keep secrets out of the image let one
in, with database credentials, and `.gitignore` had the mirror of it: `.env.*`
with a `!.env.example` negation that rescues only the exact name, silently
ignoring an example file meant to be committed.

Both were found by looking at the **artefact** — what ended up inside the image,
what git actually tracks — rather than by re-reading the rules. Re-reading the
rules is what missed them in the first place.

## Verify before claiming

Run the thing and read the output. "Should work" is not a result, and neither
is a CI badge nobody opened. When reporting a fix, report the command and what
it printed.

This includes the tools used to check things. A background command's exit code is
the exit code of the last thing in the pipe, not of the interesting command — a
`docker build` that failed 127 on `corepack enable` was reported as exit 0 because
`tail` succeeded. Print the code you actually mean.

Count what you mean, too. A merge guard counted `FAIL` in a CI log and refused a
green run: the two matches were ClickHouse's `AUTHENTICATION_FAILED`, from a drill
where authentication was meant to fail. Count check-result lines, anchored, never a
substring of the whole log.

## A failed check sets the exit code where it fails

In a verifier, `check()` sets `process.exitCode = 1` the moment an assertion
fails — not only in a summary at the end.

Two search verifiers printed FAIL and exited 0. Each had a `return` inside a `try`
for the case where its service never started; the `finally` ran, the function
returned, and the end-of-run `if (failures > 0)` gate below it never executed. CI
would have reported a run that could not even start the thing under test as green.

## A fixture has to be able to show the failure

A negative result is only evidence if the setup could have produced the positive
one. Chapters 6 and 7 found five setups that could not:

- Two tenants on **different shards**. A search through tenant A's alias only
  visits A's shard, so it could not have returned B's documents even with the
  alias filter deleted. Tenants in the isolation tests are now chosen, by asking
  the cluster, to share a shard.
- The first probe of whether a Point-in-Time keeps an alias filter made the same
  mistake, and would have credited the filter with what routing did.
- "A title match outranks a description match" stayed green with the field
  boosts deleted, because BM25 already favours short fields. The fixture now needs
  the boost to win.
- A search loop's control required more than 20 iterations, and passed with 22
  on a quick run. It now requires answers from both sides of the swap it watches.
- A part-count metric that reported a table's total passed for one that found the
  worst partition, because every fixture row sat in one month and the two numbers
  were equal. A row in a second partition made the break visible: 52 against 51.

Ask what the run would look like if the guard were missing. If the answer is
"the same", the fixture is wrong, not the guard.

## A module everything imports must not require what only one caller needs

Search's OpenSearch client loaded the whole service configuration, and every
script and verifier that talks to the cluster imports that client. So each
variable added for the HTTP or Kafka side became one a bootstrap script had to be
given: `SCHEMA_REGISTRY_URL` broke them all in one PR, and `JWT_ISSUER`,
`JWT_AUDIENCE` and `EVENT_SERVICE_URL` broke them all again in the next.
The cluster settings now live in `cluster-env.ts`, which is all the client reads.

## Derive a list from its source; never copy it

A hand-written copy of a list falls behind the list, silently. In this repository:

- a verifier's environment for spawning Search fell behind the service three
  times — it is now read from Search's own `.env.example`
- `verify-env-example.mjs` never booted Live or Search, and
  `verify-env-documented.mjs` kept its own service list — both now check against
  `services/`
- the Dockerfile's manifest list missed Live — `verify-dockerfile-workspaces.mjs`

Where a list must stay hand-written, check it against what it describes.

## Check what owns a port before trusting its answer

Compose application containers run with `restart: unless-stopped`, so after a
Docker restart they come back — built from whatever image was last built. A
freshly built Ticketing crashed with `EADDRINUSE` on 3003 while `/health` on 3003
answered 200 from the old container, and the DLQ verifier then failed against code
this branch had already changed. Stop the container a test needs to replace, run
the fresh build, and start the container again afterwards.

## Watch the effect, not the call

A call whose name promises what you want can do nothing in your configuration.

Analytics' ingest committed its Kafka offsets with kafkajs's
`commitOffsetsIfNecessary()`. With `autoCommit` off, that call with no argument
waits for an interval or a threshold, both null, and commits nothing, ever. The code
read correctly and every row landed. The group's committed offsets sat at -1, so
every restart would have replayed whatever `earliest` or `latest` chose.

It was found by asserting the effect: reading the committed offsets after the
write, not trusting that the commit was called.

## Identity comes from the message, and includes the tenant

At-least-once delivery means the same message arrives twice, and a materialized
view has already aggregated both copies by the time anyone looks. So deduplication
happens at the write, keyed on what the message is, not where it was read:

- A token built from a batch's offset range double-counted a redelivery that ended
  later than the first attempt, and could never catch a republish at a new offset.
- Keyed on the message id alone, one tenant's message silently erased another's
  under a colliding id. Kafka is not tenant-scoped, and an id is only as unique as
  its producer makes it.

## Refuse at the edge what the store would silently change

ClickHouse 25.8 refuses few bad values and stores many as something else, without
an error. Measured:

- 70000 in a UInt16 was stored as 4464, and 2^64 in a UInt64 as 384.
- `2026-02-30` became 2 March.
- A garbage time became 1900-01-01.

Nothing downstream can see a value that was changed on the way in. So the mapper
refuses anything the column cannot hold: ill-formed strings, instants that do not
round-trip through `toISOString`, and amounts past 2^53.

## Tell the row's fault from the store's

A failed write is the data's fault or the store's, and the two need opposite
handling. Poison is kept aside and the partition moves on. An outage is thrown,
nothing is committed, and the batch is redelivered.

Wrong one way, a partition stalls forever. A lone surrogate made ClickHouse refuse
a whole batch, and rethrowing that as an outage redelivered it indefinitely. That
happened twice: once in the activity rows, then again in the dead-letter row's own
error text, which echoed the surrogate.

Wrong the other way, data is lost. A mutation that parked a registry outage as
poison committed past a purchase that never came back.

Classify by what was measured, here the `CANNOT_PARSE_*` family, and keep
dead-letter text well-formed.

## Measure at the size production will send

Every check passed on batches of tens of rows. A duplicate lookup over 10000 ids
failed with `Field value too long`: each ClickHouse query parameter travels as one
HTTP form field, capped at `http_max_field_value_size` (128 KiB). That is not a
refusal of the rows, so a consumer catching up on a backlog would have rethrown its
first large batch forever. No review or test found it. Running the writer once at
backlog scale did.

## Check the index with EXPLAIN, not by reading the query

`(tenant_id, event_id) IN (...)` looks as if it narrows by the sort key's first two
columns. On ClickHouse 25.8, `EXPLAIN indexes = 1` shows it using `event_id` only,
whether the list is a parameter or literals, so it reads every tenant's granules.
Two separate `IN` lists use both columns.

## A cleanup filtered by the policy it ignores deletes nothing, quietly

`verify-community-foundation.mjs` ended with a DELETE per tenant per table as the
owner, and then counted what was left. Both ran with **no `app.tenant_id` set**.
FORCE row security binds the owner, so the DELETE matched zero rows — and the
count asked through the same filter, saw an empty table, and reported "0 left".
A cleanup that works and a cleanup that deletes nothing were indistinguishable,
and the error was swallowed by `.catch(() => {})`.

It ran that way for several chapters. Every run left two `OutboxMessage` rows on
community.feed with a payload of `{}`, which no relay can ever encode. Eight rows
and four abandoned runs later, a relay pass in a *different* verifier claimed one,
dead-lettered it, exited 1, and failed CI — which is the only reason anyone
looked. Locally it stayed hidden, because compose's relay-community had already
dead-lettered them; CI starts infrastructure only, so there they were still
waiting.

Measured, with the delete reverted: `0 deleted, 22 left`. Fixed: `22 deleted,
0 left`.

Two rules fall out. A cleanup runs in the same context the writes ran in. And it
reports what it **deleted**, not only what remains — `0 deleted, 0 left` passing
is exactly the bug, so the delete count is the positive control.

## The compose stack may be doing your verifier's work for it

A verifier ran the relay itself with `--once` and asserted the pass exited 0. That
control was worth nothing twice over: the exit code is 0 for a pass that found
nothing to do, and `relay-community` is a compose service draining the *same*
outbox, so with the stack up the row is published whether the verifier's pass runs
or not.

Deleting the call to `relayOnce()` entirely left every assertion below it green.
A control that survives the removal of the thing it controls for is decoration.

The fix is to assert the effect on the row — `publishedAt` is set for THIS message
id — rather than the behaviour of the process the verifier happens to have started.
Same family as checking what owns a port before trusting its answer.

## Timestamps come from the database, and are compared there

Prisma maps `DateTime` to `timestamp WITHOUT time zone`, and node-postgres reads
such a column as LOCAL time and writes a JS `Date` into it as local wall time.
Containers run UTC, so this is invisible in compose and in CI — and a verifier
spawns services on the HOST, which here is UTC+8.

Community's outbox payload carried `occurredAt: new Date().toISOString()` while
the schema said it was the row's commit time and consumers order on it. It was
really the app's clock: measured 2 ms from the row's own `createdAt` on one
machine with both clocks correct, and the gap across services is whatever their
clocks disagree by. It is stamped in the same statement now, from the same
`now()` the row defaults to.

Then the *assertion* for it made the mirror-image mistake — reading `createdAt`
into JS and comparing `Date.parse()` — and reported an eight-hour difference, to
the millisecond, on a correct service. It would have failed here and passed on
CI. The comparison happens in SQL now; Postgres comparing its own column to its
own rendering has no timezone to get wrong.

The rest of the repository is already safe because it does all of this in SQL:
`now()`, `now() + ($4 || ' seconds')::interval`, `extract(epoch from now() - ...)`.
Keep it that way — never pass a JS `Date` into one of these columns, and never
compare one in JS.

## The right value for the wrong reason is not a passing test

`community.feed` was added to `create-topics.mjs` asking for
`retention.ms=604800000`, and to `verify-topic-configs.mjs` asserting the same
number. It went green immediately. Nothing had been applied: Kafka's
`log.retention.hours=168` **is** 604800000, so the check was reading the broker
default back and agreeing with itself.

Both halves had the same blindness, and both were written by the events.activity.dlq
bug at the top of that file:

- the verifier compared `configValue` only, so it could not tell a value we set
  from one we were given;
- `create-topics.mjs` measured drift the same way, so the config was never
  counted as drifted and `alterConfigs` was never called. It printed
  "topic configs already match this file".

`describeConfigs` answers this directly — `isDefault: false` means the value is
set on the topic. Measured across every expectation in that file: all
`isDefault=false` except the one that was wrong. After the fix the script printed
`reconciled community.feed: retention.ms 604800000 (broker default) -> 604800000`,
which is the whole bug in one line.

When an expected value coincides with a default, equality proves nothing. Assert
the provenance, not just the number.

## A closed set is a ranking control, not tidiness

Community's reactions accepted any upper-case string as a `kind`. `kind` is part of
the Reaction unique key, so two different kinds from the same user are two rows and
two increments of the counter that feeds `rankScore`. One account could invent a
kind per request and push a post's rank as far as the rate limiter allowed.

Measured before it was fixed: one user, two invented kinds, the post's count went
from one to three. The unique key looked like "one reaction per person per post"
and enforced nothing of the sort.

Where a value is part of a key, the set of values it may take is part of the
constraint.

## Readiness must ask the dependency as this process

ClickHouse answers `GET /ping` without authenticating, so a `/ready` built on
`ping()` said ready with a wrong password. `ping({ select: true })` runs a query and
fails with `AUTHENTICATION_FAILED`.

The same probe had also latched true once the consumer started, so a crash-looping
consumer answered ready. Readiness has to track the failure it exists to report.

## A probe that can match nothing proves nothing

Notification refuses to start if its sweeper credential can mark a delivery SENT. The
probe was `UPDATE "NotificationDelivery" SET status = 'SENT' WHERE status = 'PENDING'`,
and row security refuses that PER ROW. On a database with nothing in flight it matched
no rows, raised nothing, and reported that the credential could do the one thing it
must not — so every email verifier's Notification refused to start, on a system that
was merely idle.

Probe something that cannot be empty: ask the catalog whether the rule exists, or hold
a row and act on it. Same shape as `relayB.length >= 0`, which held most strongly of
nothing.

## An RLS test of WITH CHECK must not use RETURNING or WHERE

Either one makes Postgres apply the USING clause to the new row, so a policy with
`WITH CHECK (true)` stayed green. Roll such statements back rather than committing
them: with RLS broken, an unscoped UPDATE moves every visible row.

## A refusal from your relay is not a verdict from the recipient

Suppression reacts to a 5.1.x reply to RCPT TO. That reply comes from the SMTP host
this system relays through — a smarthost with a stale recipient table, a half-finished
MX migration — not from the recipient's own server. The design note claimed otherwise,
and with no expiry that would have been a permanent password-reset lockout for every
user of a misconfigured domain. Every suppression now ends, and each new bounce extends
it.

Addresses are less regular than a pattern assumes, too: `mary.o'connor@corp.example` is
one Auth accepts, and a redaction pattern treating `'` as a delimiter published the
name into logs, rows and Kafka headers.

## `pnpm run <script> -- a b` hands the script the `--`

On every shell except PowerShell, where pnpm.ps1 swallows it first. The documented
`suppression:remove -- <tenant> <address>` therefore read the tenant as `--` and told
the operator the address was not suppressed — the one answer that must not be wrong
while a user is locked out. `dlq:replay` had it too. Strip a leading `--` before
reading arguments, and have a verifier run the DOCUMENTED form, not just the file.

## Count check results with an anchor and a control

A merge guard counted `FAIL` lines in a CI log with a pattern built for the wrong
format. It found 0 failures, and also 0 passes: zero failures out of zero lines is not
a green run. Anchor on the timestamp (`\dZ\s+ok {4}\S`), and print the ok count as the
control every time — 972 on #109, 1043 on #112.

## Retrying a partial batch API never succeeds

kafkajs's `admin.deleteGroups(list)` deletes what it can and throws for the rest.
Retried with the same list, it then fails on the groups it already deleted, for ever,
so a verifier's cleanup reported "left to expire" on every run. Delete one at a time,
and judge by the effect: whether the cluster still lists the group.

## Measure the container before blaming the process

Notification exited 137 on `docker compose stop`, and the conclusion written down was
"shutdown outlives the grace period". It did not: every container on that machine had
been created with `Config.StopTimeout=1`, and the stop took 1.7 s. An explicit
`stop_grace_period` fixed it, and the shutdown then took 4.6 s and exited 0. The
measurement — `docker inspect` and a timed stop — cost two minutes and pointed
somewhere else entirely.

## Searches that quietly look at nothing

Each of these returned "no matches" from a search that was not running:

- a `*/src/**` or brace glob matches nothing; use `**/src/**`, or pass the path
- ripgrep skips `node_modules` when handed a directory, because .gitignore excludes it:
  pass the exact file, or use `Select-String`
- PowerShell 5.1's `Set-Content -Encoding utf8` writes a BOM, which Prisma rejects as
  an invalid schema
- a here-string can stop being one on a stray trailing space, and the shell then splits
  a commit message into pathspecs. `git commit -F <file>` has no quoting to get wrong.

Treat "no matches" from a narrow search as no evidence at all, and prove the search
works by making it match something.
