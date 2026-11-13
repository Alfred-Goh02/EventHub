import { createFileRoute, Link } from "@tanstack/react-router";
import { useState, type FormEvent } from "react";
import { Loader2, MailCheck } from "lucide-react";
import { api, getTenantName } from "@/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { ErrorNotice, FieldError } from "@/components/app/ErrorNotice";
import { AccountFrame, accountCard } from "@/components/app/AccountFrame";

export const Route = createFileRoute("/forgot-password")({
  ssr: false,
  head: () => ({
    meta: [
      { title: "Reset your password — EventHub" },
      { name: "description", content: "Get a link to choose a new password." },
    ],
  }),
  component: ForgotPasswordPage,
});

function ForgotPasswordPage() {
  const [email, setEmail] = useState("");
  const [sentTo, setSentTo] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      await api().forgotPassword(email);
      setSentTo(email);
    } catch (err) {
      setError(err);
    } finally {
      setBusy(false);
    }
  };

  // Auth answers every address the same way, so that nobody can use this page to find
  // out who has an account. The page does too: it never says whether one exists.
  if (sentTo) {
    return (
      <AccountFrame title="Check your email" subtitle={getTenantName()}>
        <div className={accountCard}>
          <MailCheck className="h-6 w-6 text-primary" />
          <p className="text-sm">
            If <span className="font-medium">{sentTo}</span> has an account here, a link to choose a
            new password is on its way.
          </p>
          <p className="text-sm text-muted-foreground">
            The link works once, and not for long. If it has expired by the time you open it, ask
            for another one here.
          </p>
          <div className="flex justify-between text-sm">
            <button
              type="button"
              className="text-muted-foreground underline-offset-4 hover:underline"
              onClick={() => setSentTo(null)}
            >
              Use a different email
            </button>
            <Link
              to="/login"
              search={{ redirect: "/" }}
              className="underline-offset-4 hover:underline"
            >
              Back to sign in
            </Link>
          </div>
        </div>
      </AccountFrame>
    );
  }

  return (
    <AccountFrame
      title="Reset your password"
      subtitle="Enter the email you sign in with, and we'll send you a link to choose a new password."
    >
      <form onSubmit={submit} className={accountCard}>
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
        <ErrorNotice error={error} />
        <Button type="submit" className="w-full" disabled={busy}>
          {busy && <Loader2 className="h-4 w-4 animate-spin" />}
          Send the link
        </Button>
        <p className="text-center text-sm">
          <Link
            to="/login"
            search={{ redirect: "/" }}
            className="text-muted-foreground underline-offset-4 hover:underline"
          >
            Back to sign in
          </Link>
        </p>
      </form>
    </AccountFrame>
  );
}
