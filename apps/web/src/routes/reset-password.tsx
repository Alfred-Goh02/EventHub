import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import { useQueryClient } from "@tanstack/react-query";
import { useCallback, useState, type FormEvent } from "react";
import { Loader2 } from "lucide-react";
import { api, ApiError, clearTokens, getTenantName } from "@/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { ErrorNotice, FieldError } from "@/components/app/ErrorNotice";
import { AccountFrame, accountCard } from "@/components/app/AccountFrame";
import { linkSearch, MIN_PASSWORD_LENGTH, passwordProblem, useLinkToken } from "@/lib/account";

export const Route = createFileRoute("/reset-password")({
  ssr: false,
  validateSearch: linkSearch,
  head: () => ({
    meta: [
      { title: "Choose a new password — EventHub" },
      // The page's address held a credential when it opened: send it to nobody.
      { name: "referrer", content: "no-referrer" },
    ],
  }),
  component: ResetPasswordPage,
});

function ResetPasswordPage() {
  const search = Route.useSearch();
  const navigate = useNavigate();
  const qc = useQueryClient();
  const clear = useCallback(
    () => void navigate({ to: "/reset-password", search: {}, replace: true }),
    [navigate],
  );
  const token = useLinkToken(search.token, clear);
  const [password, setPassword] = useState("");
  const [confirm, setConfirm] = useState("");
  const [problem, setProblem] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);

  if (!token) {
    return (
      <AccountFrame title="Choose a new password" subtitle={getTenantName()}>
        <div className={accountCard}>
          <p className="text-sm">
            This page needs the link from your password reset email. Open it from the email again,
            or ask for a new one.
          </p>
          <Link to="/forgot-password" className="text-sm underline-offset-4 hover:underline">
            Ask for a new link
          </Link>
        </div>
      </AccountFrame>
    );
  }

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    const p = passwordProblem(password, confirm);
    setProblem(p);
    if (p) return;
    setBusy(true);
    setError(null);
    try {
      await api().resetPassword(token, password);
      // Auth has signed this account out everywhere, this browser included.
      clearTokens();
      qc.clear();
      navigate({
        to: "/login",
        search: { redirect: "/", notice: "password-changed" },
        replace: true,
      });
    } catch (err) {
      setError(err);
    } finally {
      setBusy(false);
    }
  };

  const spent = error instanceof ApiError && error.status === 400 && !error.fieldErrors["password"];

  if (spent) {
    return (
      <AccountFrame title="This link has run out" subtitle={getTenantName()}>
        <div className={accountCard}>
          <p className="text-sm">
            It has expired, or it has already been used. A reset link works once, and not for long.
          </p>
          <Link to="/forgot-password" className="text-sm underline-offset-4 hover:underline">
            Ask for a new link
          </Link>
        </div>
      </AccountFrame>
    );
  }

  return (
    <AccountFrame title="Choose a new password" subtitle={getTenantName()}>
      <form onSubmit={submit} className={accountCard} noValidate>
        <div className="space-y-1.5">
          <Label htmlFor="password">New password</Label>
          <Input
            id="password"
            type="password"
            autoComplete="new-password"
            minLength={MIN_PASSWORD_LENGTH}
            required
            value={password}
            onChange={(e) => setPassword(e.target.value)}
          />
          <p className="text-xs text-muted-foreground">
            At least {MIN_PASSWORD_LENGTH} characters.
          </p>
          <FieldError error={error} field="password" />
        </div>
        <div className="space-y-1.5">
          <Label htmlFor="confirm">Type it again</Label>
          <Input
            id="confirm"
            type="password"
            autoComplete="new-password"
            required
            value={confirm}
            onChange={(e) => setConfirm(e.target.value)}
          />
        </div>
        {problem && (
          <p role="alert" className="text-sm text-destructive">
            {problem}
          </p>
        )}
        <ErrorNotice error={error} />
        <Button type="submit" className="w-full" disabled={busy}>
          {busy && <Loader2 className="h-4 w-4 animate-spin" />}
          Change password
        </Button>
        <p className="text-xs text-muted-foreground">
          Changing it signs you out on every device. You'll sign in again with the new one.
        </p>
      </form>
    </AccountFrame>
  );
}
