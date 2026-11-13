import { createFileRoute, Link } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { CheckCircle2, Clock, Loader2 } from "lucide-react";
import { api, ApiError, type Reservation } from "@/api";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { ErrorNotice } from "@/components/app/ErrorNotice";
import { StatusBadge } from "@/components/app/StatusBadge";
import { formatCountdown, formatDateTime, formatMoney } from "@/lib/format";
import { settleAwaitedPayment } from "@/lib/payment";

export const Route = createFileRoute("/_app/checkout/$reservationId")({
  head: () => ({
    meta: [
      { title: "Checkout — EventHub" },
      { name: "description", content: "Review and pay for your reserved tickets." },
      { property: "og:title", content: "Checkout — EventHub" },
      { property: "og:description", content: "Review and pay for your reserved tickets." },
    ],
  }),
  component: CheckoutPage,
});

type Notice = { kind: "info" | "error"; text: string; error?: unknown } | null;

function CheckoutPage() {
  const { reservationId } = Route.useParams();
  const [paying, setPaying] = useState(false);
  /** The attempt /pay answered 202 for, until a snapshot that names it settles it. */
  const [awaitedPayment, setAwaitedPayment] = useState<string | null>(null);
  const awaitingBank = awaitedPayment !== null;
  const [notice, setNotice] = useState<Notice>(null);

  const q = useQuery({
    queryKey: ["reservation", reservationId],
    queryFn: () => api().getReservation(reservationId),
    refetchInterval: (query) => {
      const s = query.state.data?.status;
      if (awaitingBank || s === "PENDING_PAYMENT") return 3000;
      if (s === "HELD") return 15000;
      return false;
    },
  });
  const r = q.data;

  // Countdown from the server's holdSecondsLeft, ticked locally.
  const [left, setLeft] = useState<number | null>(null);
  useEffect(() => {
    if (r?.status === "HELD") setLeft(r.holdSecondsLeft);
    else setLeft(null);
  }, [r, q.dataUpdatedAt]);
  useEffect(() => {
    if (left === null) return;
    if (left <= 0) {
      q.refetch();
      return;
    }
    const t = setTimeout(() => setLeft((s) => (s === null ? s : s - 1)), 1000);
    return () => clearTimeout(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [left]);

  // Resolve the "confirming" state from polling, but only from a snapshot that knows the
  // attempt (see settleAwaitedPayment).
  useEffect(() => {
    if (!awaitedPayment || !r) return;
    const outcome = settleAwaitedPayment(r, awaitedPayment);
    if (outcome === "waiting") return;
    setAwaitedPayment(null);
    if (outcome === "failed") {
      setNotice({ kind: "error", text: "Your payment didn't go through; you can try again." });
    }
  }, [r, awaitedPayment]);

  const pay = async () => {
    setPaying(true);
    setNotice(null);
    try {
      const res = await api().payReservation(reservationId);
      if (res.httpStatus === 202) setAwaitedPayment(res.body.paymentId);
      await q.refetch();
    } catch (e) {
      if (e instanceof ApiError && e.status === 402) {
        setNotice({ kind: "error", text: "Your card was declined. You can try again.", error: e });
      } else if (e instanceof ApiError && e.isNetwork) {
        setNotice({
          kind: "info",
          text: "We lost the connection while paying. Here's the latest status of your order.",
        });
      } else {
        setNotice({
          kind: "error",
          text: e instanceof ApiError ? e.message : "Payment failed.",
          error: e,
        });
      }
      await q.refetch();
    } finally {
      setPaying(false);
    }
  };

  if (q.isPending) return <Skeleton className="h-72 w-full rounded-xl" />;
  if (q.isError && !r) {
    const nf = q.error instanceof ApiError && q.error.status === 404;
    return (
      <ErrorNotice
        error={q.error}
        message={nf ? "We couldn't find this reservation." : undefined}
      />
    );
  }
  if (!r) return null;

  if (r.status === "CONFIRMED") return <Success r={r} />;

  const confirming = awaitingBank || r.status === "PENDING_PAYMENT";

  return (
    <div className="mx-auto max-w-xl space-y-6">
      <div>
        <p className="text-sm text-muted-foreground">Checkout</p>
        <h1 className="text-3xl font-semibold">{r.event.title}</h1>
        <p className="mt-1 text-muted-foreground">{formatDateTime(r.event.startsAt)}</p>
      </div>

      {r.status === "HELD" && left !== null && (
        <div className="flex items-center gap-3 rounded-xl border bg-accent/50 px-4 py-3">
          <Clock className="h-5 w-5 text-accent-foreground" />
          <div className="text-sm">
            Your tickets are held for{" "}
            <span className="font-semibold tabular-nums">{formatCountdown(Math.max(0, left))}</span>
          </div>
        </div>
      )}

      <OrderSummary r={r} />

      {notice && notice.kind === "error" && (
        <ErrorNotice error={notice.error ?? true} message={notice.text} />
      )}
      {notice && notice.kind === "info" && (
        <p className="rounded-lg border bg-muted px-4 py-3 text-sm">{notice.text}</p>
      )}
      {q.isError && <ErrorNotice error={q.error} />}

      {confirming && (
        <div className="flex items-center gap-3 rounded-xl border px-4 py-4">
          <Loader2 className="h-5 w-5 animate-spin text-primary" />
          <div>
            <p className="font-medium">Confirming your payment…</p>
            <p className="text-sm text-muted-foreground">
              The bank is still answering. This page updates by itself.
            </p>
          </div>
        </div>
      )}

      {r.status === "HELD" && !confirming && (
        <Button size="lg" className="w-full" disabled={paying} onClick={pay}>
          {paying ? (
            <>
              <Loader2 className="h-4 w-4 animate-spin" /> Processing payment…
            </>
          ) : (
            `Pay ${formatMoney(r.totalCents)}`
          )}
        </Button>
      )}

      {(r.status === "EXPIRED" || r.status === "CANCELLED") && (
        <div className="space-y-3 rounded-xl border p-5 text-center">
          <p className="font-medium">
            {r.status === "EXPIRED" ? "This hold has expired." : "This reservation was cancelled."}
          </p>
          <Button asChild variant="outline">
            <Link to="/events/$id" params={{ id: r.event.id }}>
              Back to the event
            </Link>
          </Button>
        </div>
      )}
    </div>
  );
}

function OrderSummary({ r }: { r: Reservation }) {
  return (
    <div className="rounded-xl border bg-card p-5">
      <div className="mb-3 flex items-center justify-between">
        <h2 className="text-lg font-semibold">Your order</h2>
        <StatusBadge status={r.status} />
      </div>
      <ul className="space-y-1 text-sm">
        {r.items.map((i) => (
          <li key={i.ticketTypeId} className="flex justify-between gap-2">
            <span>
              {i.quantity} × {i.name}
            </span>
            <span className="tabular-nums">{formatMoney(i.unitPriceCents * i.quantity)}</span>
          </li>
        ))}
      </ul>
      <div className="mt-3 flex justify-between border-t pt-3 font-semibold">
        <span>Total</span>
        <span className="tabular-nums">{formatMoney(r.totalCents)}</span>
      </div>
    </div>
  );
}

function Success({ r }: { r: Reservation }) {
  return (
    <div className="mx-auto max-w-xl space-y-6 text-center">
      <CheckCircle2 className="mx-auto h-14 w-14 text-success" />
      <div>
        <h1 className="text-3xl font-semibold">You're going!</h1>
        <p className="mt-2 text-muted-foreground">
          Payment received for {r.event.title} on {formatDateTime(r.event.startsAt)}.
        </p>
      </div>
      <div className="text-left">
        <OrderSummary r={r} />
      </div>
      <Button asChild>
        <Link to="/tickets" search={{ tab: "all" }}>
          Go to My tickets
        </Link>
      </Button>
    </div>
  );
}
