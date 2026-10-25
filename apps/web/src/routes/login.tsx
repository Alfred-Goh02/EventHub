import { createFileRoute, Link, redirect, useNavigate } from "@tanstack/react-router";
import { useQueryClient } from "@tanstack/react-query";
import { useState, type FormEvent } from "react";
import { CheckCircle2, Loader2 } from "lucide-react";
import { api, ApiError, getSession, getTenantName, isMockHost, writeTokens } from "@/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { ErrorNotice, FieldError, retryText } from "@/components/app/ErrorNotice";
import { AccountFrame, accountCard } from "@/components/app/AccountFrame";

const safeRedirect = (r: unknown) =>
  typeof r === "string" && r.startsWith("/") && !r.startsWith("//") ? r : "/";

/** Why the account pages sent someone here. */
const NOTICES = {
  "password-changed":
    "Your password has changed. Sign in with the new one. Every other device was signed out.",
  "account-created": "Your account is ready. Sign in with the password you just chose.",
} as const;
type Notice = keyof typeof NOTICES;
const isNotice = (n: unknown): n is Notice => typeof n === "string" && n in NOTICES;

export const Route = createFileRoute("/login")({
  ssr: false,
  validateSearch: (s: Record<string, unknown>): { redirect: string; notice?: Notice } => ({
    redirect: safeRedirect(s["redirect"]),
    ...(isNotice(s["notice"]) ? { notice: s["notice"] } : {}),
  }),
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
  const { redirect: to, notice } = Route.useSearch();
  const navigate = useNavigate();
  const qc = useQueryClient();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      const tokens = await api().login(email, password);
      // A new session starts from an empty cache, whoever was signed in before.
      qc.clear();
      writeTokens(tokens);
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
    <AccountFrame title={getTenantName()} subtitle="Sign in to find events and get tickets.">
      <form onSubmit={submit} className={accountCard}>
        {notice && !error && (
          <p role="status" className="flex gap-2 text-sm">
            <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0 text-primary" />
            {NOTICES[notice]}
          </p>
        )}
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
          <div className="flex items-baseline justify-between">
            <Label htmlFor="password">Password</Label>
            <Link
              to="/forgot-password"
              className="text-xs text-muted-foreground underline-offset-4 hover:underline"
            >
              Forgot your password?
            </Link>
          </div>
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
    </AccountFrame>
  );
}
