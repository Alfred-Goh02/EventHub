# Lovable prompt 1 — the attendee app: sign in, browse, buy, my tickets

Status: sent to Lovable on 2026-10-24, approved by Alfred. Lovable built it in its own
TanStack Start template rather than the React Router the prompt asks for, ran out of credits
as it finished, and synced the result to `Alfred-Goh02/eventhub-ticketing`. It now lives in
`apps/web`, which is the source of truth.

Everything below the line is what was sent (nested bullets were typed without their
indentation, because Lovable's editor turns indentation into formatting).

---

Build the web app for **EventHub**, a multi-tenant event-ticketing platform. The backend already exists and must not be changed or mocked away. Use React, TypeScript, Vite, Tailwind and shadcn/ui, and React Router. Keep it clean and calm: a light and a dark theme, responsive down to phone width. This first version is the **attendee** experience only: signing in, browsing events, reserving and paying for tickets, and seeing what you bought.

## How the app talks to the backend

**Tenants are subdomains.** Each organisation has its own subdomain, and the app is always opened on it: `http://acme.eventhub.localhost:5173` is the tenant `acme`. Read the tenant from the first label of `window.location.hostname`. The API is the same host on port 8000: `http://acme.eventhub.localhost:8000`. Make the API origin come from `import.meta.env.VITE_API_ORIGIN` when set, otherwise `${location.protocol}//${location.hostname}:8000`. Never send a tenant header. The server takes the tenant from the host, and any other header fails CORS. The only request headers allowed are `Authorization`, `Content-Type`, `Idempotency-Key`, `If-Match` and `X-Request-ID`.

**One API client.** Put every call in `src/api/` as typed functions, and let no component call `fetch` directly. Also write a **mock implementation of the same interface**, with realistic sample data, used automatically when the page is NOT on a `*.eventhub.localhost` or `*.eventhub.com` host. That is how this preview works without the backend. The real client is used everywhere else. The types are the contract below; keep them exactly as written.

**Sign-in and tokens. There are no cookies.**
- `POST /auth/login` with `{ email, password }` returns `{ accessToken, refreshToken }`.
- The access token is a JWT that lives 15 minutes. Decode its payload (no verification needed client-side) to get `sub` (user id), `tenantSlug`, `role` (`ADMIN` | `ORGANIZER` | `ATTENDEE`) and `exp`.
- Store both tokens in `localStorage`. Send `Authorization: Bearer <accessToken>` on every other call.
- `POST /auth/refresh` with `{ refreshToken }` returns a NEW pair, and the old refresh token is dead. Presenting a dead one signs the user out everywhere, so refresh must be **single-flight across tabs**:
  - take a Web Locks lock (`navigator.locks.request('eventhub-refresh', ...)`);
  - inside it, re-read `localStorage` (another tab may have refreshed already);
  - refresh only if the stored token is still the one you hold;
  - listen for `storage` events so every tab picks up new tokens.
- Refresh about a minute before `exp`, and also on any 401: refresh once, retry the request once, and if the refresh itself answers 401, clear the tokens and go to the sign-in page. Never refresh on 403.
- `POST /auth/logout` with `{ refreshToken }` answers 204. Then clear both tokens.
- If the decoded `tenantSlug` is not the current subdomain, treat the user as signed out.

**Errors come in three shapes. Render all of them.**
- `{ "error": "message" }`
- `{ "error": { "formErrors": string[], "fieldErrors": { "<field>": string[] } } }`: validation errors. Show `fieldErrors` next to their fields.
- The gateway's own `{ "message": "..." }`, for example on 401, 404, 413 and 429.

Every response has an `X-Request-ID` header. Show it in small text under any error ("Reference: …") so a failure can be found in the logs. On 429, read `Retry-After` (seconds) and tell the user when to try again. Money is integer cents, and the currency is always SGD: show `S$25.00` for `2500`. Times are ISO-8601 UTC strings; show them in the browser's local time.

## Pages

1. **Sign in** (`/login`): email and password. A 401 means "Email or password is wrong" (the server never says which). A 429 means "Too many attempts", with the wait time. After signing in, go back to where the user was heading.
2. **Events** (`/`): published events, soonest first.
   - Calls `GET /events?status=PUBLISHED&limit=20`, which returns `{ data: Event[], nextCursor: string | null }`.
   - "Load more" calls the same with `&cursor=<nextCursor>` until `nextCursor` is null.
   - `Event` = `{ id, title, slug, status, capacity, startsAt, endsAt, version, organiserId }`.
   - Show title, date and time; "Past" for an event whose `endsAt` has passed. Empty state when there are none.
3. **Event** (`/events/:id`):
   - `GET /events/:id` returns `{ id, title, slug, description: string | null, status, capacity, startsAt, endsAt, version, organiserId, venueId, createdAt }`. A 404 means "This event doesn't exist or isn't public."
   - Tickets come from `GET /ticket-types?eventId=<id>`, which returns `{ data: [{ id, eventId, name, priceCents, total, remaining, held, sold, revision }] }`, cheapest first.
   - For each type show the name, the price, and "N left" from `remaining` ("Sold out" at 0), with a quantity stepper from 0 to min(20, remaining).
   - A summary shows the total.
   - **Reserve** sends `POST /reservations` with header `Idempotency-Key: <uuid>` and body `{ eventId, items: [{ ticketTypeId, quantity }] }`, only lines with quantity > 0, at most 10 lines.
     - Generate the key once per checkout attempt and REUSE it when retrying the same cart after a network error, 502, 503 or 504. Use a new key when the cart changes.
     - 201 returns `{ id, eventId, status: "HELD", expiresAt, items }`. Go to the checkout page.
     - On 503, with `Retry-After`, or the error "this service has not caught up with recent events yet; please retry", retry automatically after that many seconds, up to 3 times, with the same key.
     - On 409, show the server's `error` text. It is written for people: for example "not enough tickets remaining" (refresh the ticket list) or "event is not on sale".
     - On 422 (key reused with a different cart), make a new key and try again once.
4. **Checkout** (`/checkout/:reservationId`):
   - `GET /reservations/:id` returns:
     - `{ id, status, event: { id, title, slug, startsAt, endsAt }, items: [{ ticketTypeId, name, quantity, unitPriceCents }], totalCents, currency, createdAt, holdExpiresAt, holdSecondsLeft, confirmedAt, payment: { id, status, amountCents, currency } | null }`.
     - `status` is `HELD` | `PENDING_PAYMENT` | `CONFIRMED` | `EXPIRED` | `CANCELLED`. `payment.status` is `PENDING` | `SUCCEEDED` | `FAILED` | `UNKNOWN`.
   - While `HELD`, show a countdown from `holdSecondsLeft`. That number comes from the server's clock; do NOT compute it from `holdExpiresAt` and the browser's clock. Tick it down locally, and re-read the reservation every 15 s and at 0.
   - **Pay** sends `POST /reservations/:id/pay` (no body). It can take up to ~10 s, so disable the button and show progress.
     - 200: `{ status: "CONFIRMED", amountCents, ... }`. Show a success screen and link to My tickets.
     - 202: `{ status: "PENDING", message }`. The bank hasn't answered yet; this is NOT a failure. Show "Confirming your payment…" and poll `GET /reservations/:id` every 3 s until `status` is `CONFIRMED`, or back to `HELD` (then say "Your payment didn't go through; you can try again"), or `EXPIRED`.
     - 402: `{ status: "HELD", error: "payment was declined" }`. The hold is kept, so say "Your card was declined" and let them pay again.
     - 409: show the server's `error`. Then re-read the reservation and show its real state.
     - After a network error during pay, do NOT pay again blindly. Re-read the reservation first.
   - Show `EXPIRED` as "This hold has expired", with a button back to the event.
5. **My tickets** (`/tickets`):
   - `GET /reservations?limit=20` returns `{ data: Reservation[], nextCursor }`, newest first. "Load more" sends `&cursor=`.
   - Tabs: All, Confirmed (`&status=CONFIRMED`), Held (`&status=HELD`). Keep the status when paging.
   - Each row: event title and date, the items, the total, and a status badge. Clicking a row opens the checkout or detail page above.

## Layout

A top bar shows the tenant name (the subdomain, capitalised), links to Events and My tickets, and a user menu with the role and "Sign out". Every page except sign-in requires a session.

Do not build organiser tools, live Q&A, community or account pages (register, forgot password) yet. They come in later prompts, so leave room for them in the navigation.
