// API contract. Keep these shapes exactly as the backend defines them.

export type Role = "ADMIN" | "ORGANIZER" | "ATTENDEE";

export interface TokenPair {
  accessToken: string;
  refreshToken: string;
}

export interface AccessClaims {
  sub: string;
  tenantSlug: string;
  role: Role;
  /** Seconds since the epoch, on the SERVER's clock, like exp. */
  iat?: number;
  exp: number;
}

export interface Page<T> {
  data: T[];
  nextCursor: string | null;
}

export type EventStatus = string;

export interface Event {
  id: string;
  title: string;
  slug: string;
  status: EventStatus;
  capacity: number;
  startsAt: string;
  endsAt: string;
  version: number;
  organiserId: string;
}

export interface EventDetail {
  id: string;
  title: string;
  slug: string;
  description: string | null;
  status: EventStatus;
  capacity: number;
  startsAt: string;
  endsAt: string;
  version: number;
  organiserId: string;
  venueId: string;
  createdAt: string;
}

export interface TicketType {
  id: string;
  eventId: string;
  name: string;
  priceCents: number;
  total: number;
  remaining: number;
  held: number;
  sold: number;
  revision: number;
}

export interface ReservationRequestItem {
  ticketTypeId: string;
  quantity: number;
}

export interface CreateReservationBody {
  eventId: string;
  items: ReservationRequestItem[];
}

export interface CreatedReservation {
  id: string;
  eventId: string;
  status: "HELD";
  expiresAt: string;
  items: ReservationRequestItem[];
}

export type ReservationStatus = "HELD" | "PENDING_PAYMENT" | "CONFIRMED" | "EXPIRED" | "CANCELLED";
export type PaymentStatus = "PENDING" | "SUCCEEDED" | "FAILED" | "UNKNOWN";

export interface Reservation {
  id: string;
  status: ReservationStatus;
  event: { id: string; title: string; slug: string; startsAt: string; endsAt: string };
  items: { ticketTypeId: string; name: string; quantity: number; unitPriceCents: number }[];
  totalCents: number;
  currency: string;
  createdAt: string;
  /** Both null unless the reservation is HELD: a paid or lapsed hold has no deadline. */
  holdExpiresAt: string | null;
  holdSecondsLeft: number | null;
  confirmedAt: string | null;
  payment: { id: string; status: PaymentStatus; amountCents: number; currency: string } | null;
}

export type PayResult =
  | { httpStatus: 200; body: { status: "CONFIRMED"; amountCents: number; [k: string]: unknown } }
  | { httpStatus: 202; body: { status: "PENDING"; paymentId: string; message: string } };

/** POST /auth/register: the account an invitation created. */
export interface RegisteredUser {
  id: string;
  email: string;
  role: Role;
}

export interface EventHubApi {
  login(email: string, password: string): Promise<TokenPair>;
  /** Revokes this login's refresh-token family. The caller has already dropped the tokens. */
  logout(refreshToken: string): Promise<void>;
  /** Always resolves the same way, account or no account: Auth answers 202 to every address. */
  forgotPassword(email: string): Promise<void>;
  /** Spends a reset link's token. Every session of that account is signed out. */
  resetPassword(token: string, password: string): Promise<void>;
  /** Spends an invitation's token. The tenant and the address come from the invitation. */
  register(token: string, password: string): Promise<RegisteredUser>;
  listEvents(cursor?: string | null): Promise<Page<Event>>;
  getEvent(id: string): Promise<EventDetail>;
  listTicketTypes(eventId: string): Promise<{ data: TicketType[] }>;
  createReservation(
    body: CreateReservationBody,
    idempotencyKey: string,
  ): Promise<CreatedReservation>;
  getReservation(id: string): Promise<Reservation>;
  payReservation(id: string): Promise<PayResult>;
  listReservations(opts: {
    status?: ReservationStatus | undefined;
    cursor?: string | null;
  }): Promise<Page<Reservation>>;
}
