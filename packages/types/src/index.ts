/**
 * The shared vocabulary across HTTP and Kafka boundaries.
 *
 * Everything here is a CONTRACT. Once a second service imports one of these,
 * changing it means coordinating a deploy across every consumer — so these
 * definitions must describe what is actually on the wire, not what would be
 * convenient. A shared type that disagrees with reality is worse than no type,
 * because consumers trust it.
 */

/** Who a user is allowed to be within a tenant. */
export type Role = 'ADMIN' | 'ORGANIZER' | 'ATTENDEE';

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

export type ReservationStatus =
  | 'PENDING'
  | 'CONFIRMED'
  | 'EXPIRED'
  | 'CANCELLED';

export interface TicketReservedEvent {
  reservationId: string;
  eventId: string;
  ticketTypeId: string;
  userId: string;
  quantity: number;
  expiresAt: string;
}
