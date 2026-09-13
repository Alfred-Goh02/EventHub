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

## A failed check sets the exit code where it fails

In a verifier, `check()` sets `process.exitCode = 1` the moment an assertion
fails — not only in a summary at the end.

Two search verifiers printed FAIL and exited 0. Each had a `return` inside a `try`
for the case where its service never started; the `finally` ran, the function
returned, and the end-of-run `if (failures > 0)` gate below it never executed. CI
would have reported a run that could not even start the thing under test as green.

## A fixture has to be able to show the failure

A negative result is only evidence if the setup could have produced the positive
one. Chapter 6 found four setups that could not:

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
