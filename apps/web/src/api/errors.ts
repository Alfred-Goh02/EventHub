export interface FieldErrors {
  [field: string]: string[];
}

/** Normalised error for all three server error shapes, plus network failures (status 0). */
export class ApiError extends Error {
  status: number;
  requestId: string | null;
  retryAfterSeconds: number | null;
  formErrors: string[];
  fieldErrors: FieldErrors;
  body: unknown;

  constructor(opts: {
    status: number;
    message: string;
    requestId?: string | null;
    retryAfterSeconds?: number | null;
    formErrors?: string[];
    fieldErrors?: FieldErrors;
    body?: unknown;
  }) {
    super(opts.message);
    this.status = opts.status;
    this.requestId = opts.requestId ?? null;
    this.retryAfterSeconds = opts.retryAfterSeconds ?? null;
    this.formErrors = opts.formErrors ?? [];
    this.fieldErrors = opts.fieldErrors ?? {};
    this.body = opts.body;
  }

  get isNetwork() {
    return this.status === 0;
  }
}

export function parseRetryAfter(v: string | null): number | null {
  if (!v) return null;
  const n = Number(v);
  if (Number.isFinite(n)) return Math.max(0, Math.ceil(n));
  const d = Date.parse(v);
  return Number.isNaN(d) ? null : Math.max(0, Math.ceil((d - Date.now()) / 1000));
}

export function errorFromBody(
  status: number,
  body: unknown,
  requestId: string | null,
  retryAfterSeconds: number | null,
): ApiError {
  let message = `Request failed (${status})`;
  let formErrors: string[] = [];
  let fieldErrors: FieldErrors = {};
  if (body && typeof body === "object") {
    const b = body as Record<string, unknown>;
    if (typeof b.error === "string") message = b.error;
    else if (b.error && typeof b.error === "object") {
      const e = b.error as { formErrors?: string[]; fieldErrors?: FieldErrors };
      formErrors = e.formErrors ?? [];
      fieldErrors = e.fieldErrors ?? {};
      message = formErrors[0] ?? "Please check the highlighted fields.";
    } else if (typeof b.message === "string") message = b.message;
  }
  return new ApiError({
    status,
    message,
    requestId,
    retryAfterSeconds,
    formErrors,
    fieldErrors,
    body,
  });
}
