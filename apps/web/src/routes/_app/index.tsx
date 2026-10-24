import { createFileRoute, Link } from "@tanstack/react-router";
import { useInfiniteQuery } from "@tanstack/react-query";
import { CalendarDays, ChevronRight, Loader2 } from "lucide-react";
import { api } from "@/api";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { Badge } from "@/components/ui/badge";
import { ErrorNotice } from "@/components/app/ErrorNotice";
import { formatDateTime, formatTime, isPast } from "@/lib/format";

export const Route = createFileRoute("/_app/")({
  head: () => ({
    meta: [
      { title: "Events — EventHub" },
      { name: "description", content: "Browse upcoming published events and reserve tickets." },
      { property: "og:title", content: "Events — EventHub" },
      {
        property: "og:description",
        content: "Browse upcoming published events and reserve tickets.",
      },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary" },
    ],
  }),
  component: EventsPage,
});

function EventsPage() {
  const q = useInfiniteQuery({
    queryKey: ["events"],
    queryFn: ({ pageParam }) => api().listEvents(pageParam),
    initialPageParam: null as string | null,
    getNextPageParam: (p) => p.nextCursor,
  });
  const events = q.data?.pages.flatMap((p) => p.data) ?? [];

  return (
    <div>
      <h1 className="text-3xl font-semibold">Events</h1>
      <p className="mt-1 text-muted-foreground">What's coming up, soonest first.</p>

      <div className="mt-6 space-y-2">
        {q.isPending &&
          Array.from({ length: 5 }).map((_, i) => (
            <Skeleton key={i} className="h-20 w-full rounded-xl" />
          ))}
        {q.isError && <ErrorNotice error={q.error} />}
        {q.isSuccess && events.length === 0 && (
          <div className="rounded-xl border border-dashed p-10 text-center text-muted-foreground">
            <CalendarDays className="mx-auto mb-2 h-6 w-6" />
            No events are on sale right now. Check back soon.
          </div>
        )}
        {events.map((e) => {
          const past = isPast(e.endsAt);
          return (
            <Link
              key={e.id}
              to="/events/$id"
              params={{ id: e.id }}
              className="group flex items-center gap-4 rounded-xl border bg-card p-4 transition-colors hover:border-primary/40"
            >
              <div className="flex h-14 w-14 shrink-0 flex-col items-center justify-center rounded-lg bg-accent text-accent-foreground">
                <span className="text-[10px] font-semibold uppercase tracking-wide">
                  {new Date(e.startsAt).toLocaleDateString(undefined, { month: "short" })}
                </span>
                <span className="font-display text-xl leading-none">
                  {new Date(e.startsAt).getDate()}
                </span>
              </div>
              <div className="min-w-0 flex-1">
                <div className="flex items-center gap-2">
                  <h2 className="truncate font-sans text-base font-semibold">{e.title}</h2>
                  {past && <Badge variant="muted">Past</Badge>}
                </div>
                <p className="text-sm text-muted-foreground">
                  {formatDateTime(e.startsAt)} – {formatTime(e.endsAt)}
                </p>
              </div>
              <ChevronRight className="h-4 w-4 text-muted-foreground transition-transform group-hover:translate-x-0.5" />
            </Link>
          );
        })}
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
