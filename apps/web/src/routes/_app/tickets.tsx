import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import { useInfiniteQuery } from "@tanstack/react-query";
import { Loader2, Ticket } from "lucide-react";
import { api, type ReservationStatus } from "@/api";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { ErrorNotice } from "@/components/app/ErrorNotice";
import { StatusBadge } from "@/components/app/StatusBadge";
import { formatDateTime, formatMoney } from "@/lib/format";

type Tab = "all" | "confirmed" | "held";
const STATUS: Record<Tab, ReservationStatus | undefined> = {
  all: undefined,
  confirmed: "CONFIRMED",
  held: "HELD",
};

export const Route = createFileRoute("/_app/tickets")({
  validateSearch: (s: Record<string, unknown>): { tab: Tab } => ({
    tab: s["tab"] === "confirmed" || s["tab"] === "held" ? s["tab"] : "all",
  }),
  head: () => ({
    meta: [
      { title: "My tickets — EventHub" },
      { name: "description", content: "Your reservations and confirmed tickets." },
      { property: "og:title", content: "My tickets — EventHub" },
      { property: "og:description", content: "Your reservations and confirmed tickets." },
    ],
  }),
  component: TicketsPage,
});

function TicketsPage() {
  const { tab } = Route.useSearch();
  const navigate = useNavigate({ from: "/tickets" });
  const status = STATUS[tab];
  const q = useInfiniteQuery({
    queryKey: ["reservations", status ?? "all"],
    queryFn: ({ pageParam }) => api().listReservations({ status, cursor: pageParam }),
    initialPageParam: null as string | null,
    getNextPageParam: (p) => p.nextCursor,
  });
  const rows = q.data?.pages.flatMap((p) => p.data) ?? [];

  return (
    <div>
      <h1 className="text-3xl font-semibold">My tickets</h1>
      <Tabs
        value={tab}
        onValueChange={(v) => navigate({ search: { tab: v as Tab } })}
        className="mt-6"
      >
        <TabsList>
          <TabsTrigger value="all">All</TabsTrigger>
          <TabsTrigger value="confirmed">Confirmed</TabsTrigger>
          <TabsTrigger value="held">Held</TabsTrigger>
        </TabsList>
      </Tabs>

      <div className="mt-4 space-y-2">
        {q.isPending &&
          Array.from({ length: 3 }).map((_, i) => (
            <Skeleton key={i} className="h-24 w-full rounded-xl" />
          ))}
        {q.isError && <ErrorNotice error={q.error} />}
        {q.isSuccess && rows.length === 0 && (
          <div className="rounded-xl border border-dashed p-10 text-center text-muted-foreground">
            <Ticket className="mx-auto mb-2 h-6 w-6" />
            Nothing here yet.{" "}
            <Link to="/" className="text-primary underline-offset-4 hover:underline">
              Browse events
            </Link>
          </div>
        )}
        {rows.map((r) => (
          <Link
            key={r.id}
            to="/checkout/$reservationId"
            params={{ reservationId: r.id }}
            className="block rounded-xl border bg-card p-4 transition-colors hover:border-primary/40"
          >
            <div className="flex flex-wrap items-start justify-between gap-2">
              <div className="min-w-0">
                <p className="font-semibold">{r.event.title}</p>
                <p className="text-sm text-muted-foreground">{formatDateTime(r.event.startsAt)}</p>
              </div>
              <StatusBadge status={r.status} />
            </div>
            <div className="mt-2 flex flex-wrap items-end justify-between gap-2 text-sm">
              <span className="text-muted-foreground">
                {r.items.map((i) => `${i.quantity} × ${i.name}`).join(", ")}
              </span>
              <span className="font-medium tabular-nums">{formatMoney(r.totalCents)}</span>
            </div>
          </Link>
        ))}
      </div>

      {q.isFetchNextPageError && <ErrorNotice error={q.error} className="mt-4" />}
      {q.hasNextPage && (
        <div className="mt-6 flex justify-center">
          <Button
            variant="outline"
            onClick={() => q.fetchNextPage()}
            disabled={q.isFetchingNextPage}
          >
            {q.isFetchingNextPage && <Loader2 className="h-4 w-4 animate-spin" />}
            Load more
          </Button>
        </div>
      )}
    </div>
  );
}
