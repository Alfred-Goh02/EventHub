/**
 * The shared vocabulary across HTTP and Kafka boundaries.
 *
 * Everything here is a CONTRACT. Once a second service imports one of these,
 * changing it means coordinating a deploy across every consumer — so these
 * definitions must describe what is actually on the wire, not what would be
 * convenient. A shared type that disagrees with reality is worse than no type,
 * because consumers trust it.
 *
 * That paragraph was already here, and two of the three types below disagreed
 * with reality anyway:
 *
 *   ReservationStatus   declared PENDING, which is not a state this system has,
 *                       and omitted HELD and PENDING_PAYMENT, which are the two
 *                       the entire seat and payment design turns on.
 *   TicketReservedEvent had no tenantId — in a system whose every row-level
 *                       policy reads app.tenant_id — no state, and flattened the
 *                       items ARRAY into a single ticketTypeId and quantity, so
 *                       it could not describe a two-line cart.
 *
 * Neither was imported anywhere yet, which is the only reason nothing broke.
 * They would have been wrong for the first consumer to trust them, and the
 * failure would have looked like a bug in that consumer.
 *
 * The rule this file now follows: a type here either has NO other source of
 * truth, or it is derived from the one it has. Nothing is hand-copied from a
 * schema that a generator already reads.
 */

/**
 * Who a user is allowed to be within a tenant.
 *
 * The ARRAY is the definition and the union is derived from it, not the other
 * way round. auth-middleware needs a runtime list to validate an incoming
 * token's claim, and it used to keep its own copy — so adding a role to the
 * union and forgetting the array would have compiled cleanly and then rejected
 * every request carrying the new role, with a 401, in every service at once.
 *
 * Mirrors the Role enum in services/auth/prisma/schema.prisma, which is the
 * source of truth; scripts/verify-shared-types.mjs checks they still agree.
 */
export const ROLES = ['ADMIN', 'ORGANIZER', 'ATTENDEE'] as const;

export type Role = (typeof ROLES)[number];

/** Runtime narrowing for a value off the wire, derived from the same list. */
export const isRole = (v: unknown): v is Role =>
  typeof v === 'string' && (ROLES as readonly string[]).includes(v);

/**
 * The access token payload, exactly as Auth signs it.
 *
 * `sub` rather than `userId` because it is the registered JWT claim for the
 * subject — every JWT library already understands it, and inventing a
 * synonym discards that for nothing.
 */
export interface JwtPayload {
  /** User id. Registered claim: subject. */
  sub: string;
  /** Which tenant this user belongs to. Custom claim. */
  tenantId: string;
  /** Authorization role within that tenant. Custom claim. */
  role: Role;
  /** Who minted the token. Verifiers must require this. */
  iss: string;
  /** Who the token is for. Stops replay against a different service. */
  aud: string;
  /** Issued at, seconds since epoch. */
  iat: number;
  /** Expires at, seconds since epoch. */
  exp: number;
}

/**
 * The state of a seat hold, as the Ticketing database records it.
 *
 * Mirrors the ReservationStatus enum in
 * services/ticketing/prisma/schema.prisma, which is the source of truth —
 * Prisma generates a client-side enum from it, but that client is not
 * importable from another service, which is why this exists at all.
 *
 * HELD and PENDING_PAYMENT are the two that matter and the two that were
 * missing. The distinction between them is the whole of the payment design:
 * a HELD reservation's clock is running and the sweeper may reclaim its seats;
 * a PENDING_PAYMENT one is mid-charge and the sweeper must leave it alone, or a
 * customer pays for seats that were resold underneath them.
 *
 * scripts/verify-shared-types.mjs checks this against the schema.
 */
export type ReservationStatus =
  | 'HELD'
  | 'PENDING_PAYMENT'
  | 'CONFIRMED'
  | 'EXPIRED'
  | 'CANCELLED';

/**
 * TicketReservedEvent USED TO LIVE HERE, hand-written, and was wrong.
 *
 * The tickets.reserved message is defined by
 * contracts/avro/tickets.reserved-value.avsc, and
 * scripts/generate-avro-types.mjs already turns that into TypeScript at
 * contracts/generated/tickets-reserved.ts — checked in, and re-generated and
 * diffed by CI on every push, so it cannot drift from the schema.
 *
 * A second, hand-maintained copy of the same record could only ever drift from
 * it, and had: no tenantId, no state, and items flattened from an array to a
 * single line. Deleting it is the fix. Import the generated type:
 *
 *   import type { TicketReserved } from '../../contracts/generated/tickets-reserved.js';
 *
 * The same goes for anything else on a topic with an .avsc. This file is for
 * vocabulary that has no generator behind it.
 */
