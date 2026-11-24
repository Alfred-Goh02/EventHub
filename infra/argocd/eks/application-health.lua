-- The health of an Argo CD Application, as its parent sees it. The EKS root
-- (infra/argocd/eks/root.json) waits on it between sync waves, so that setup (wave -1) has
-- finished before the services, the gateway and the web app (wave 0) are synced at all.
--
-- WHY IT IS NEEDED. Argo CD has had no health check for argoproj.io/Application since 1.8,
-- and v3.6.1 still has none. Without one, a parent counts every child as done the moment it
-- exists, and a wave waits for nothing. infra/eks/up.mjs --gitops puts this script into
-- argocd-cm, as resource.customizations.health.argoproj.io_Application, before it applies
-- the root.
--
-- WHY NOT THE DOCUMENTED ONE. Argo CD's docs restore the check by passing the child's
-- status.health through, and that answer can be about the wrong commit. On a new commit the
-- root gives the child the new revision, and until the child has fetched and compared it, its
-- status still says Synced and Healthy about the previous commit's finished Job; the wave
-- delay is two seconds, and a fetch can take longer. And an OutOfSync app whose resources are
-- missing still reports Healthy (controller/health.go: "the OutOfSync status already
-- indicates resources are missing"). So here a child is Healthy only when its status was
-- compared against the revision its spec names now, it is Synced at that revision, and its
-- own health is Healthy. Anything short of that is Progressing, which a wave waits on.
--
-- AND THE WAIT MUST END. A parent's sync has no timeout (controller.sync.timeout.seconds is
-- 0), and while it runs, auto-sync starts nothing else: a child that can never be Healthy has
-- to say Degraded, or the root would wait for ever and never take the next commit, the fix
-- included. So a child is Degraded when its sync of this revision ended Failed or Error, when
-- this revision cannot be compared at all, or when it is Synced at this revision and
-- unhealthy. Never for an EARLIER revision: that answer is stale, the same as a stale
-- Healthy, and failing the root on it would refuse the commit that fixes it.
--
-- Measured on kind, on Argo CD v3.6.1, with a child whose fetches were slowed to 15 s each:
-- on a new commit the child's status said Synced and Healthy about the previous commit for
-- 44 s, and the documented script let wave 0 through on it, 43 s before the Job finished;
-- this one held it until the Job had finished. A Job that failed ended the root's sync Failed
-- 5 min 20 s later, after the 5 retries Argo CD gives every automated sync, and wave 0 was
-- never given that commit. The next commit, the fix, went through, while its child still
-- said Degraded about the failed one for 45 s.
--
-- scripts/verify-argocd-eks.mjs runs this file through Argo CD's own CLI, at the version the
-- cluster runs, against an Application in each of these states.

local hs = {}
hs.status = "Progressing"
hs.message = ""

local want = nil
if obj.spec ~= nil and obj.spec.source ~= nil then
  want = obj.spec.source.targetRevision
end
if want == nil then
  hs.message = "no spec.source.targetRevision to wait for"
  return hs
end
local st = obj.status
if st == nil or st.sync == nil or st.health == nil then
  hs.message = "waiting for the first comparison at " .. want
  return hs
end

-- A sync of this revision that ended: nothing will try it again.
local op = st.operationState
if op ~= nil and (op.phase == "Failed" or op.phase == "Error") then
  local tried = nil
  if op.syncResult ~= nil then
    tried = op.syncResult.revision
  end
  if tried == nil and op.operation ~= nil and op.operation.sync ~= nil then
    tried = op.operation.sync.revision
  end
  if tried == want then
    hs.status = "Degraded"
    hs.message = "the sync of " .. want .. " ended " .. op.phase .. ": " .. (op.message or "")
    return hs
  end
end

-- Everything below is about the last comparison. Is it about this revision?
local compared = nil
if st.sync.comparedTo ~= nil and st.sync.comparedTo.source ~= nil then
  compared = st.sync.comparedTo.source.targetRevision
end
if compared ~= want then
  hs.message = "waiting for a comparison at " .. want .. "; the status is about " .. tostring(compared)
  return hs
end
if st.conditions ~= nil then
  for _, c in ipairs(st.conditions) do
    if c.type == "ComparisonError" or c.type == "InvalidSpecError" then
      hs.status = "Degraded"
      hs.message = want .. " cannot be compared: " .. (c.message or c.type)
      return hs
    end
  end
end
if st.sync.status ~= "Synced" or st.sync.revision ~= want then
  hs.message = "waiting for the sync of " .. want .. " (" .. tostring(st.sync.status) .. " at " .. tostring(st.sync.revision) .. ")"
  return hs
end
hs.status = st.health.status
hs.message = st.health.message or ""
return hs
