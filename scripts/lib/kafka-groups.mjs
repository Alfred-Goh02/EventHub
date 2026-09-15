/**
 * Deletes the consumer groups a run created, waiting for each one to empty.
 *
 * WHY NOT A RETRIED deleteGroups(list). kafkajs deletes every group it can and then
 * throws for the rest, typically a group whose killed consumer is still a member
 * until its session times out. Retried with the same list, the call then fails on
 * the groups it already deleted (GROUP_ID_NOT_FOUND), every time, so the cleanup in
 * verify-email-retries.mjs reported "left to expire" on every run.
 *
 * So each group is deleted on its own, and what counts is the effect: a group is
 * gone when the cluster no longer lists it, whatever the delete call said.
 *
 * Resolves to the groups still listed when time ran out; empty when all are gone.
 */
export async function deleteGroupsWhenEmpty(admin, groupIds, { timeoutMs = 45_000, stepMs = 3_000 } = {}) {
  const deadline = Date.now() + timeoutMs;
  let left = [...new Set(groupIds)];
  for (;;) {
    for (const groupId of left) await admin.deleteGroups([groupId]).catch(() => {});
    left = await listed(admin, left);
    if (left.length === 0 || Date.now() >= deadline) return left;
    await new Promise((r) => setTimeout(r, stepMs));
  }
}

/** The groups among these that the cluster still lists. An unanswered listing counts all of them as still there. */
async function listed(admin, groupIds) {
  try {
    const names = new Set((await admin.listGroups()).groups.map((g) => g.groupId));
    return groupIds.filter((g) => names.has(g));
  } catch {
    return groupIds;
  }
}
