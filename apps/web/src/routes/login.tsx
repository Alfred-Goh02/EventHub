import { createFileRoute, redirect, useNavigate } from "@tanstack/react-router";
import { useState, type FormEvent } from "react";
import { Loader2, Ticket } from "lucide-react";
import { api, ApiError, getSession, getTenantName, isMockHost, writeTokens } from "@/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { ErrorNotice, FieldError, retryText } from "@/components/app/ErrorNotice";
import { ThemeToggle } from "@/components/app/ThemeToggle";

const safeRedirect = (r: unknown) =>
  typeof r === "string" && r.startsWith("/") && !r.startsWith("//") ? r : "/";

export const Route = createFileRoute("/login")({
  ssr: false,
  validateSearch: (s: Record<string, unknown>) => ({ redirect: safeRedirect(s["redirect"]) }),
  beforeLoad: ({ search }) => {
    if (getSession()) throw redirect({ href: search.redirect });
  },
  head: () => ({
    meta: [
      { title: "Sign in — EventHub" },
      { name: "description", content: "Sign in to browse events and buy tickets on EventHub." },
      { property: "og:title", content: "Sign in — EventHub" },
      {
        property: "og:description",
        content: "Sign in to browse events and buy tickets on EventHub.",
      },
    ],
  }),
  component: LoginPage,
});

function LoginPage() {
  const { redirect: to } = Route.useSearch();
  const navigate = useNavigate();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      writeTokens(await api().login(email, password));
      navigate({ href: to, replace: true });
    } catch (err) {
      setError(err);
    } finally {
      setBusy(false);
    }
  };

  const apiErr = error instanceof ApiError ? error : null;
  const message =
    apiErr?.status === 401
      ? "Email or password is wrong."
      : apiErr?.status === 429
        ? `Too many attempts. ${retryText(apiErr) ?? "Please wait a moment and try again."}`
        : undefined;

  return (
    <div className="flex min-h-screen flex-col">
      <div className="flex justify-end p-3">
        <ThemeToggle />
      </div>
      <div className="flex flex-1 items-center justify-center px-4 pb-16">
        <div className="w-full max-w-sm">
          <div className="mb-8 text-center">
            <div className="mx-auto mb-4 flex h-12 w-12 items-center justify-center rounded-full bg-accent text-accent-foreground">
              <Ticket className="h-6 w-6" />
            </div>
            <h1 className="text-3xl font-semibold">{getTenantName()}</h1>
            <p className="mt-1 text-sm text-muted-foreground">
              Sign in to find events and get tickets.
            </p>
          </div>
          <form onSubmit={submit} className="space-y-4 rounded-xl border bg-card p-6 shadow-sm">
            <div className="space-y-1.5">
              <Label htmlFor="email">Email</Label>
              <Input
                id="email"
                type="email"
                autoComplete="email"
                required
                value={email}
                onChange={(e) => setEmail(e.target.value)}
              />
              <FieldError error={error} field="email" />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="password">Password</Label>
              <Input
                id="password"
                type="password"
                autoComplete="current-password"
                required
                value={password}
                onChange={(e) => setPassword(e.target.value)}
              />
              <FieldError error={error} field="password" />
            </div>
            <ErrorNotice error={error} message={message} />
            <Button type="submit" className="w-full" disabled={busy}>
              {busy && <Loader2 className="h-4 w-4 animate-spin" />}
              Sign in
            </Button>
            {isMockHost() && (
              <p className="text-center text-xs text-muted-foreground">
                Preview mode: any email and password works.
              </p>
            )}
          </form>
        </div>
      </div>
    </div>
  );
}
