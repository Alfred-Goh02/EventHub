import { serviceEnv } from './service-env.mjs';
import { commitAtEnd, joinedSince, membersOf } from './kafka-groups.mjs';

/**
 * The consumer groups of a Notification that a verifier starts, and keeping it to them.
 *
 * NOTIFICATION RUNS FIVE GROUPS, AND A VERIFIER MUST NAME TWO. KAFKA_CONSUMER_GROUP is
 * the dispatch group, and the service names three more from it: `-retry`, `-replies` and
 * `-replies-retry`. CONTACTS_CONSUMER_GROUP is the contact-card copy's, on
 * auth.user-contact, and is named on its own.
 *
 * Every Notification verifier but one set the first and not the second, so each ran
 * inside the REAL notification-contacts group while it was up. Measured on
 * verify-email-delivery.mjs: its Notification joined notification-contacts as the only
 * member, and a card written while it ran moved the real group's committed offset on
 * that partition from 140 to 141. The deployed Notification resumes from there, so a
 * card a throwaway process read, before it was killed, is one the real one never reads.
 *
 * A GROUP OF THE RUN'S OWN STARTS AT THE BEGINNING, and the service has no setting to
 * say otherwise: the topic is compacted precisely so a new group reads every card. So a
 * verifier's own contacts group replayed all of auth.user-contact, and dead-lettered
 * again every forged card that earlier runs of verify-contact-cards.mjs had left on it:
 * measured, 77 entries on auth.user-contact.dlq from one run of verify-reply-emails.mjs,
 * on a dead-letter topic that is kept. So the run's contacts group is committed at the
 * end of the topic before the service starts, and reads only what is written after.
 *
 * Use, in a verifier that starts Notification:
 *
 *   const GROUPS = notificationGroups(`verify-something-${RUN}`);
 *   serviceEnv(ROOT, 'notification', { ...GROUPS.env, ... })
 *   const contacts = await contactsFromTheEnd(admin, ROOT, GROUPS);   // before it starts
 *   const own = await contacts.joined();                               // once it is up
 *   check(own.ok, own.message);
 *   deleteGroupsWhenEmpty(admin, [...GROUPS.all, ...])                 // at the end
 */

/** The topic Notification's contact-card consumer reads, from services/notification/src/contacts/consumer.ts. */
export const CONTACTS_TOPIC = 'auth.user-contact';

/** Every group a Notification with these two names joins: the three it derives from the dispatch group's name, too. */
const family = (dispatch, contacts) => [dispatch, `${dispatch}-retry`, `${dispatch}-replies`, `${dispatch}-replies-retry`, contacts];

/**
 * This run's groups, named from `base`: the environment that gives them to the service,
 * the contacts group's name, and every group the service will join, for the cleanup.
 */
export function notificationGroups(base) {
  const contacts = `${base}-contacts`;
  return {
    env: { KAFKA_CONSUMER_GROUP: base, CONTACTS_CONSUMER_GROUP: contacts },
    contacts,
    all: family(base, contacts),
  };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Call BEFORE the service starts. Commits the run's contacts group at the end of
 * auth.user-contact, read back, and notes who is in each of the real groups now: the
 * ones Notification's .env.example names, which are the ones compose runs.
 *
 * Resolves to { positions, joined }. joined(), called once the service is up, waits for
 * the run's contacts group to gain a member and then names every member the real groups
 * gained meanwhile. A compose Notification already running is in the "before", so a
 * verifier that runs beside one still passes; a member that arrived with this run's
 * process does not.
 */
export async function contactsFromTheEnd(admin, root, groups) {
  const example = serviceEnv(root, 'notification');
  const real = family(example.KAFKA_CONSUMER_GROUP, example.CONTACTS_CONSUMER_GROUP);
  const positions = (await commitAtEnd(admin, groups.contacts, [CONTACTS_TOPIC]))[CONTACTS_TOPIC];
  const before = await membersOf(admin, real);

  async function joined(timeoutMs = 60_000) {
    const deadline = Date.now() + timeoutMs;
    let ours = 0;
    for (;;) {
      ours = (await membersOf(admin, [groups.contacts])).get(groups.contacts)?.size ?? 0;
      if (ours > 0 || Date.now() >= deadline) break;
      await sleep(500);
    }
    const intruders = joinedSince(before, await membersOf(admin, real));
    const ok = ours > 0 && intruders.length === 0;
    const message = ok
      ? `its contact consumer joined this run's own group, committed at the end of ${CONTACTS_TOPIC} (${positions.join(',')}), ` +
        `and none of the real groups gained a member (${real.join(', ')})`
      : [
          ours > 0 ? '' : `its contact consumer never joined this run's group ${groups.contacts}`,
          intruders.length === 0
            ? ''
            : `a real group GAINED ${intruders.join(', ')}: this run's process consumes as the deployed Notification, ` +
              'taking its partitions and committing its offsets past what it reads',
        ]
          .filter(Boolean)
          .join('; ');
    return { ok, ours, intruders, message };
  }

  return { positions, joined };
}
