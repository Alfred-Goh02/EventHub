import { Badge } from "@/components/ui/badge";
import type { ReservationStatus } from "@/api";

const MAP: Record<
  ReservationStatus,
  { label: string; v: "success" | "warning" | "muted" | "danger" }
> = {
  CONFIRMED: { label: "Confirmed", v: "success" },
  HELD: { label: "Held", v: "warning" },
  PENDING_PAYMENT: { label: "Confirming payment", v: "warning" },
  EXPIRED: { label: "Expired", v: "muted" },
  CANCELLED: { label: "Cancelled", v: "danger" },
};

export function StatusBadge({ status }: { status: ReservationStatus }) {
  const m = MAP[status] ?? { label: status, v: "muted" as const };
  return <Badge variant={m.v}>{m.label}</Badge>;
}
