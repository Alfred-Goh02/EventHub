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
  holdExpiresAt: string;
  holdSecondsLeft: number;
  confirmedAt: string | null;
  payment: { id: string; status: PaymentStatus; amountCents: number; currency: string } | null;
}

export type PayResult =
  | { httpStatus: 200; body: { status: "CONFIRMED"; amountCents: number; [k: string]: unknown } }
  | { httpStatus: 202; body: { status: "PENDING"; message: string } };

export interface EventHubApi {
  login(email: string, password: string): Promise<TokenPair>;
  logout(): Promise<void>;
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
