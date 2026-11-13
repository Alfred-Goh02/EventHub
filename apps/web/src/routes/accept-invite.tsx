import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import { useQueryClient } from "@tanstack/react-query";
import { useCallback, useState, type FormEvent } from "react";
import { Loader2 } from "lucide-react";
import { api, ApiError, getTenantName, writeTokens } from "@/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { ErrorNotice, FieldError } from "@/components/app/ErrorNotice";
import { AccountFrame, accountCard } from "@/components/app/AccountFrame";
import { linkSearch, MIN_PASSWORD_LENGTH, passwordProblem, useLinkToken } from "@/lib/account";

export const Route = createFileRoute("/accept-invite")({
  ssr: false,
  validateSearch: linkSearch,
  head: () => ({
    meta: [
      { title: "Accept your invitation — EventHub" },
      // The page's address held a credential when it opened: send it to nobody.
      { name: "referrer", content: "no-referrer" },
    ],
  }),
  component: AcceptInvitePage,
});

function AcceptInvitePage() {
  const search = Route.useSearch();
  const navigate = useNavigate();
  const qc = useQueryClient();
  const clear = useCallback(
    () => void navigate({ to: "/accept-invite", search: {}, replace: true }),
    [navigate],
  );
  const token = useLinkToken(search.token, clear);
  const [password, setPassword] = useState("");
  const [confirm, setConfirm] = useState("");
  const [problem, setProblem] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const tenant = getTenantName();

  if (!token) {
    return (
      <AccountFrame title={`Join ${tenant}`} subtitle="Accept your invitation">
        <div className={accountCard}>
          <p className="text-sm">
            This page needs the link from your invitation email. Open it from the email again.
          </p>
          <Link
            to="/login"
            search={{ redirect: "/" }}
            className="text-sm underline-offset-4 hover:underline"
          >
            I already have an account
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
    let created;
    try {
      created = await api().register(token, password);
    } catch (err) {
      setError(err);
      setBusy(false);
      return;
    }
    // The account exists now. Sign straight in on this site; if that does not work (an
    // invitation from another tenant, opened here), the account is still there to sign in to.
    try {
      const tokens = await api().login(created.email, password);
      qc.clear();
      writeTokens(tokens);
      navigate({ to: "/", replace: true });
    } catch {
      navigate({
        to: "/login",
        search: { redirect: "/", notice: "account-created" },
        replace: true,
      });
    }
  };

  const apiErr = error instanceof ApiError ? error : null;
  if (apiErr?.status === 409) {
    return (
      <AccountFrame title="You already have an account" subtitle={tenant}>
        <div className={accountCard}>
          <p className="text-sm">
            There is already an account for the address this invitation was sent to. Sign in with
            it, or reset its password if you've forgotten it.
          </p>
          <div className="flex justify-between text-sm">
            <Link
              to="/login"
              search={{ redirect: "/" }}
              className="underline-offset-4 hover:underline"
            >
              Sign in
            </Link>
            <Link to="/forgot-password" className="underline-offset-4 hover:underline">
              Reset your password
            </Link>
          </div>
        </div>
      </AccountFrame>
    );
  }
  if (apiErr?.status === 400 && !apiErr.fieldErrors["password"]) {
    return (
      <AccountFrame title="This invitation has run out" subtitle={tenant}>
        <div className={accountCard}>
          <p className="text-sm">
            It has expired, been withdrawn, or already been used. Ask whoever invited you to send a
            new one.
          </p>
        </div>
      </AccountFrame>
    );
  }

  return (
    <AccountFrame title={`Join ${tenant}`} subtitle="Choose a password to finish your account.">
      <form onSubmit={submit} className={accountCard} noValidate>
        <div className="space-y-1.5">
          <Label htmlFor="password">Password</Label>
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
          Create account
        </Button>
      </form>
    </AccountFrame>
  );
}
