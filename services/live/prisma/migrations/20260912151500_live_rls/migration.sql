-- Row-level security for Live's two tables.
--
-- Prisma never emits RLS, so every table this service owns gets its policy in a
-- hand-written migration like this one. The grants are here too, and explicitly
-- per table rather than through ALTER DEFAULT PRIVILEGES: that way a table whose
-- migration forgot its RLS block is also a table the app role cannot touch, so
-- the service fails loudly with "permission denied" instead of quietly serving
-- one tenant's questions to another. Fail closed.

-- Exactly the privileges each table needs and no more.
--
-- Question is inserted (a new question), selected (the list and the snapshot
-- endpoint), and updated (voteCount increments). It is never deleted by the
-- service, so DELETE is not granted.
GRANT SELECT, INSERT, UPDATE ON "Question" TO eventhub_live_app;

-- QuestionVote is inserted and read. An upvote is not undoable in this chapter,
-- so no UPDATE and no DELETE.
GRANT SELECT, INSERT ON "QuestionVote" TO eventhub_live_app;

-- ENABLE turns policies on. FORCE makes them apply to the table OWNER too.
--
-- Without FORCE the owner silently bypasses its own policies, which is how a
-- migration or a backfill run as the owner sees every tenant's rows while
-- looking completely correct. The service connects as eventhub_live_app, which
-- owns nothing, so the policies bind either way — FORCE is what protects the
-- other path.
ALTER TABLE "Question"     ENABLE ROW LEVEL SECURITY;
ALTER TABLE "Question"     FORCE  ROW LEVEL SECURITY;
ALTER TABLE "QuestionVote" ENABLE ROW LEVEL SECURITY;
ALTER TABLE "QuestionVote" FORCE  ROW LEVEL SECURITY;

-- USING filters rows that already exist; WITH CHECK constrains rows being
-- written. Without WITH CHECK a caller could INSERT a row belonging to another
-- tenant, or UPDATE one of its own rows to move it out — silent corruption
-- rather than a leak, so nothing alerts.
--
-- current_setting(..., true) returns NULL rather than raising when the setting
-- is absent, and NULL = anything is NULL, which is not true. So a connection
-- that never ran set_config sees zero rows instead of an error — fail closed,
-- and the reason the runtime role must never be the owner.
CREATE POLICY tenant_isolation ON "Question"
  USING ("tenantId" = current_setting('app.tenant_id', true))
  WITH CHECK ("tenantId" = current_setting('app.tenant_id', true));

CREATE POLICY tenant_isolation ON "QuestionVote"
  USING ("tenantId" = current_setting('app.tenant_id', true))
  WITH CHECK ("tenantId" = current_setting('app.tenant_id', true));
