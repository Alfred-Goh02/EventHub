import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useMemo, useRef, useState } from "react";
import { ArrowLeft, Loader2, Minus, Plus } from "lucide-react";
import { api, ApiError, type CreateReservationBody } from "@/api";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { ErrorNotice } from "@/components/app/ErrorNotice";
import { formatDateTime, formatMoney, formatTime, isPast } from "@/lib/format";
import { isRetryableTransport, reserve, type KeyState } from "@/lib/reserve";

export const Route = createFileRoute("/_app/events/$id")({
  head: () => ({
    meta: [
      { title: "Event — EventHub" },
      { name: "description", content: "Event details and tickets on EventHub." },
      { property: "og:title", content: "Event — EventHub" },
      { property: "og:description", content: "Event details and tickets on EventHub." },
    ],
  }),
  component: EventPage,
});

function EventPage() {
  const { id } = Route.useParams();
  const navigate = useNavigate();
  const qc = useQueryClient();
  const ev = useQuery({ queryKey: ["event", id], queryFn: () => api().getEvent(id), retry: false });
  const tt = useQuery({ queryKey: ["ticket-types", id], queryFn: () => api().listTicketTypes(id) });
  const [qty, setQty] = useState<Record<string, number>>({});
  const [busy, setBusy] = useState(false);
  const [waiting, setWaiting] = useState<string | null>(null);
  const [error, setError] = useState<unknown>(null);
  const keyRef = useRef<KeyState | null>(null);

  const types = useMemo(
    () => [...(tt.data?.data ?? [])].sort((a, b) => a.priceCents - b.priceCents),
    [tt.data],
  );
  const qtyOf = (tid: string) => qty[tid] ?? 0;
  const lines = types.filter((t) => qtyOf(t.id) > 0);
  const total = lines.reduce((s, t) => s + t.priceCents * qtyOf(t.id), 0);
  const count = lines.reduce((s, t) => s + qtyOf(t.id), 0);

  const set = (tid: string, n: number, max: number) => {
    setQty((q) => ({ ...q, [tid]: Math.max(0, Math.min(max, n)) }));
    setError(null);
  };

  const onReserve = async () => {
    const body: CreateReservationBody = {
      eventId: id,
      items: lines.slice(0, 10).map((t) => ({ ticketTypeId: t.id, quantity: qtyOf(t.id) })),
    };
    setBusy(true);
    setError(null);
    try {
      const r = await reserve(body, keyRef, (s, n) =>
        setWaiting(`Still syncing — retrying in ${s} s (attempt ${n} of 3)…`),
      );
      navigate({ to: "/checkout/$reservationId", params: { reservationId: r.id } });
    } catch (e) {
      setError(e);
      if (e instanceof ApiError && e.status === 409)
        qc.invalidateQueries({ queryKey: ["ticket-types", id] });
    } finally {
      setBusy(false);
      setWaiting(null);
    }
  };

  if (ev.isPending) return <Skeleton className="h-64 w-full rounded-xl" />;
  if (ev.isError) {
    const nf = ev.error instanceof ApiError && ev.error.status === 404;
    return (
      <div className="space-y-4">
        <BackLink />
        <ErrorNotice
          error={ev.error}
          message={nf ? "This event doesn't exist or isn't public." : undefined}
        />
      </div>
    );
  }
  const e = ev.data;
  const past = isPast(e.endsAt);

  return (
    <div className="space-y-8">
      <div>
        <BackLink />
        <div className="mt-4 flex flex-wrap items-center gap-2">
          <h1 className="text-3xl font-semibold sm:text-4xl">{e.title}</h1>
          {past && <Badge variant="muted">Past</Badge>}
        </div>
        <p className="mt-2 text-muted-foreground">
          {formatDateTime(e.startsAt)} – {formatTime(e.endsAt)}
        </p>
        {e.description && <p className="mt-4 max-w-2xl leading-relaxed">{e.description}</p>}
      </div>

      <div className="grid gap-6 lg:grid-cols-[1fr_300px]">
        <section className="space-y-2">
          <h2 className="text-xl font-semibold">Tickets</h2>
          {tt.isPending && <Skeleton className="h-32 w-full rounded-xl" />}
          {tt.isError && <ErrorNotice error={tt.error} />}
          {tt.isSuccess && types.length === 0 && (
            <p className="rounded-xl border border-dashed p-6 text-center text-muted-foreground">
              No tickets yet.
            </p>
          )}
          {types.map((t) => {
            const max = Math.min(20, t.remaining);
            const n = qty[t.id] ?? 0;
            return (
              <div key={t.id} className="flex items-center gap-4 rounded-xl border bg-card p-4">
                <div className="min-w-0 flex-1">
                  <p className="font-medium">{t.name}</p>
                  <p className="text-sm text-muted-foreground">
                    {formatMoney(t.priceCents)} ·{" "}
                    {t.remaining === 0 ? (
                      <span className="text-destructive">Sold out</span>
                    ) : (
                      `${t.remaining} left`
                    )}
                  </p>
                </div>
                <div className="flex items-center gap-1">
                  <Button
                    variant="outline"
                    size="icon"
                    className="h-8 w-8"
                    aria-label={`Fewer ${t.name}`}
                    disabled={n === 0 || busy}
                    onClick={() => set(t.id, n - 1, max)}
                  >
                    <Minus className="h-3.5 w-3.5" />
                  </Button>
                  <span className="w-8 text-center tabular-nums" aria-live="polite">
                    {n}
                  </span>
                  <Button
                    variant="outline"
                    size="icon"
                    className="h-8 w-8"
                    aria-label={`More ${t.name}`}
                    disabled={n >= max || busy || past}
                    onClick={() => set(t.id, n + 1, max)}
                  >
                    <Plus className="h-3.5 w-3.5" />
                  </Button>
                </div>
              </div>
            );
          })}
        </section>

        <aside className="h-fit space-y-4 rounded-xl border bg-card p-5 lg:sticky lg:top-20">
          <h2 className="text-lg font-semibold">Summary</h2>
          {lines.length === 0 ? (
            <p className="text-sm text-muted-foreground">Choose tickets to continue.</p>
          ) : (
            <ul className="space-y-1 text-sm">
              {lines.map((t) => (
                <li key={t.id} className="flex justify-between gap-2">
                  <span>
                    {qtyOf(t.id)} × {t.name}
                  </span>
                  <span className="tabular-nums">{formatMoney(t.priceCents * qtyOf(t.id))}</span>
                </li>
              ))}
            </ul>
          )}
          <div className="flex justify-between border-t pt-3 font-semibold">
            <span>Total</span>
            <span className="tabular-nums">{formatMoney(total)}</span>
          </div>
          {waiting && <p className="text-sm text-muted-foreground">{waiting}</p>}
          <ErrorNotice error={error} />
          <Button className="w-full" disabled={count === 0 || busy || past} onClick={onReserve}>
            {busy && <Loader2 className="h-4 w-4 animate-spin" />}
            {isRetryableTransport(error) ? "Try again" : "Reserve"}
          </Button>
        </aside>
      </div>
    </div>
  );
}

function BackLink() {
  return (
    <Link
      to="/"
      className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground"
    >
      <ArrowLeft className="h-4 w-4" /> All events
    </Link>
  );
}
