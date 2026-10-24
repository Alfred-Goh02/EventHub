import { AlertCircle } from "lucide-react";
import { ApiError } from "@/api";
import { cn } from "@/lib/utils";

export function retryText(e: ApiError) {
  if (e.retryAfterSeconds === null) return null;
  const at = new Date(Date.now() + e.retryAfterSeconds * 1000).toLocaleTimeString(undefined, {
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit",
  });
  return `Try again in ${e.retryAfterSeconds} s (around ${at}).`;
}

export function ErrorNotice({
  error,
  message,
  className,
}: {
  error: unknown;
  message?: string | undefined;
  className?: string | undefined;
}) {
  if (!error) return null;
  const e = error instanceof ApiError ? error : null;
  const text =
    message ??
    (e ? (e.status === 429 ? "Too many requests." : e.message) : "Something went wrong.");
  return (
    <div
      role="alert"
      className={cn(
        "rounded-lg border border-destructive/30 bg-destructive/10 px-4 py-3 text-sm",
        className,
      )}
    >
      <div className="flex gap-2 text-destructive">
        <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
        <div className="space-y-1">
          <p className="font-medium">{text}</p>
          {e && e.formErrors.length > 1 && (
            <ul className="list-disc pl-4">
              {e.formErrors.slice(1).map((f) => (
                <li key={f}>{f}</li>
              ))}
            </ul>
          )}
          {e && e.status === 429 && retryText(e) && <p>{retryText(e)}</p>}
        </div>
      </div>
      {e?.requestId && (
        <p className="mt-2 text-xs text-muted-foreground">Reference: {e.requestId}</p>
      )}
    </div>
  );
}

export function FieldError({ error, field }: { error: unknown; field: string }) {
  if (!(error instanceof ApiError)) return null;
  const msgs = error.fieldErrors[field];
  if (!msgs?.length) return null;
  return <p className="text-xs text-destructive">{msgs.join(" ")}</p>;
}
