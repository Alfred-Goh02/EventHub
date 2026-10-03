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

/**
 * The groups among these that still have members once they have had time to empty.
 *
 * For a verifier that must not run beside a real consumer of the same topic: that
 * consumer takes the run's messages, or acts on them with its own settings, and the
 * checks then fail — or pass — on code the run never started. A consumer stopped a
 * moment ago stays a member until its session times out, so each group gets that long
 * to empty before it counts as busy. A group the cluster does not know is not busy.
 */
export async function busyGroups(admin, groupIds, { timeoutMs = 40_000, stepMs = 2_000 } = {}) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const { groups } = await admin.describeGroups([...new Set(groupIds)]);
    const busy = groups.filter((g) => g.members.length > 0).map((g) => g.groupId);
    if (busy.length === 0 || Date.now() >= deadline) return busy;
    await new Promise((r) => setTimeout(r, stepMs));
  }
}

/**
 * Commits `groupId` at the current end of each topic, and reads the commit back.
 *
 * For a group new to this run whose consumer would otherwise start at the beginning. A
 * consumer resumes from a committed offset whatever it was told about where to start,
 * so this is how a verifier starts a service's consumer where the topic ends when the
 * service has no setting for it, or ignores the one it has for a new group.
 *
 * THE EFFECT IS READ BACK, not trusted: the offsets the group holds afterwards must be
 * exactly the ends that were set, or this throws. A commit that quietly did nothing
 * would otherwise look the same as one that worked, until the consumer replayed the
 * topic.
 *
 * admin.setOffsets refuses a group with members, so call this before the consumer
 * starts. It joins the group to seek, which takes about five seconds per topic.
 *
 * Resolves to each topic's committed offsets, in partition order.
 */
export async function commitAtEnd(admin, groupId, topics) {
  const set = {};
  for (const topic of topics) {
    const ends = (await admin.fetchTopicOffsets(topic)).sort((a, b) => a.partition - b.partition);
    await admin.setOffsets({ groupId, topic, partitions: ends.map((p) => ({ partition: p.partition, offset: p.high })) });
    set[topic] = ends.map((p) => String(p.high));
  }
  for (const { topic, partitions } of await admin.fetchOffsets({ groupId, topics })) {
    const read = partitions.sort((a, b) => a.partition - b.partition).map((p) => String(p.offset));
    if (read.join(',') !== set[topic].join(',')) {
      throw new Error(`${groupId} was committed at ${set[topic].join(',')} on ${topic}, and reads back ${read.join(',')}`);
    }
  }
  return set;
}

/** The current members of each group, as memberId to "clientId@host". A group the cluster does not know has none. */
export async function membersOf(admin, groupIds) {
  const { groups } = await admin.describeGroups([...new Set(groupIds)]);
  return new Map(groups.map((g) => [g.groupId, new Map(g.members.map((m) => [m.memberId, `${m.clientId}@${m.clientHost}`]))]));
}

/** Every member in `now` that was not in `before`, as "group (as clientId@host)". */
export function joinedSince(before, now) {
  const joined = [];
  for (const [groupId, members] of now) {
    for (const [memberId, who] of members) if (!before.get(groupId)?.has(memberId)) joined.push(`${groupId} (as ${who})`);
  }
  return joined.sort();
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
