import {
  createFileRoute,
  Outlet,
  redirect,
  useNavigate,
  useRouterState,
} from "@tanstack/react-router";
import { useQueryClient } from "@tanstack/react-query";
import { useEffect } from "react";
import { getSession, onForcedSignOut, startSessionLifecycle, subscribeTokens } from "@/api";
import { TopBar } from "@/components/app/TopBar";

export const Route = createFileRoute("/_app")({
  ssr: false,
  beforeLoad: ({ location }) => {
    if (!getSession()) throw redirect({ to: "/login", search: { redirect: location.href } });
  },
  component: AppLayout,
});

function AppLayout() {
  const navigate = useNavigate();
  const qc = useQueryClient();
  const href = useRouterState({ select: (s) => s.location.href });

  useEffect(() => {
    startSessionLifecycle();
    // Every way out of a session drops what was cached for it: a sign-out in another tab,
    // or a refresh Auth refused, would otherwise leave this user's reservations in the
    // cache for whoever signs in next in this tab.
    const toLogin = () => {
      qc.clear();
      navigate({
        to: "/login",
        search: { redirect: window.location.pathname + window.location.search },
        replace: true,
      });
    };
    onForcedSignOut(toLogin);
    // Another tab signed out, or tokens changed to another tenant.
    const unsub = subscribeTokens(() => {
      if (!getSession()) toLogin();
    });
    return () => {
      unsub();
    };
  }, [navigate, qc, href]);

  return (
    <div className="min-h-screen">
      <TopBar />
      <main className="mx-auto max-w-5xl px-4 py-8">
        <Outlet />
      </main>
    </div>
  );
}
