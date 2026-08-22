export interface JwtPayload {
  userId: string;
  tenantId: string;
  role: 'ADMIN' | 'ORGANIZER' | 'ATTENDEE';
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
