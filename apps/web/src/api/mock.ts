import { ApiError } from "./errors";
import { getTenantSlug } from "./env";
import { readTokens } from "./tokens";
import type { Comment, EventDetail, EventHubApi, Post, Reservation, TicketType } from "./types";

// In-memory sample backend for previews off EventHub hosts. Built lazily
// (no randomness or clocks at module scope).

interface Store {
  events: EventDetail[];
  tickets: TicketType[];
  reservations: (Reservation & { _pendingUntil?: number })[];
}
let store: Store | null = null;

const TITLES: [title: string, description: string][] = [
  ["Harbourfront Jazz Night", "An evening of live jazz by the water with three local quartets."],
  ["Design Systems Summit", "Talks and workshops on building calm, consistent interfaces."],
  ["Night Market Food Tour", "Guided walk through hawker favourites and hidden stalls."],
  ["Indie Film Screening: Monsoon", "Premiere screening followed by a director Q&A."],
  ["Sunrise Yoga at the Gardens", "Gentle flow session for all levels. Mats provided."],
  ["Startup Pitch Evening", "Ten early-stage founders, five minutes each, one winner."],
  ["Chamber Strings Recital", "Works by Dvořák and Elgar in an intimate hall."],
  ["Photography Walk: Old Town", "Shoot shophouses and street life with a pro guide."],
  ["Craft Coffee Cupping", "Taste single-origin beans from four roasters."],
  ["Kids Science Fair", "Hands-on experiments and demos for ages 6–12."],
  ["Open Mic Poetry", "Bring a poem or just listen. Every voice welcome."],
  ["Marathon Expo 2026", "Kit collection, talks and gear stalls."],
  ["Wine & Watercolour", "Paint a skyline while sampling three wines."],
  ["Tech Careers Fair", "Meet hiring teams from forty local companies."],
  ["Stand-up Comedy Showcase", "Five comics, one stage, zero mercy."],
  ["Heritage Architecture Talk", "The story behind the city's oldest buildings."],
  ["Electronic Music Rooftop", "DJs from dusk until late with skyline views."],
  ["Bookbinding Workshop", "Make your own hand-stitched notebook."],
  ["Board Games Marathon", "A hundred games, snacks and friendly strangers."],
  ["Classical Guitar Evening", "Spanish and Latin repertoire, solo and duo."],
  ["Climate Action Forum", "Panels on practical change in cities."],
  ["Pottery for Beginners", "Wheel-throwing basics in a small group."],
  ["Late-Night Museum Tour", "Galleries after hours with curator stories."],
  ["Salsa Social", "Lesson at 8, open dancing from 9."],
];

function db(): Store {
  if (store) return store;
  const now = Date.now();
  const H = 3600_000;
  const events: EventDetail[] = TITLES.map(([title, description], i) => {
    const start = now + (i - 1) * 2.3 * 24 * H + (i % 3) * 3 * H;
    return {
      id: `evt_${String(i + 1).padStart(3, "0")}`,
      title,
      slug: title.toLowerCase().replace(/[^a-z0-9]+/g, "-"),
      description,
      status: "PUBLISHED",
      capacity: 200 + i * 10,
      startsAt: new Date(start).toISOString(),
      endsAt: new Date(start + 3 * H).toISOString(),
      version: 1,
      organiserId: "org_1",
      venueId: `ven_${(i % 4) + 1}`,
      createdAt: new Date(now - 30 * 24 * H).toISOString(),
    };
  });
  const tickets: TicketType[] = [];
  events.forEach((e, i) => {
    const kinds: [string, number, number][] = [
      ["General admission", 2500 + i * 100, i % 5 === 2 ? 0 : 120 - i],
      ["Early bird", 1800, i % 2 ? 0 : 12],
      ["VIP", 8000 + i * 200, 6],
    ];
    kinds.forEach(([name, price, remaining], k) =>
      tickets.push({
        id: `tt_${e.id}_${k}`,
        eventId: e.id,
        name,
        priceCents: price,
        total: remaining + 40,
        remaining,
        held: 0,
        sold: 40,
        revision: 1,
      }),
    );
  });
  const ev = events[2];
  const tt = tickets.filter((t) => t.eventId === ev?.id);
  const first = tt[0];
  if (!ev || !first) throw new Error("mock data: the demo event needs a ticket type");
  const reservations: Store["reservations"] = [
    {
      id: "res_demo_confirmed",
      status: "CONFIRMED",
      event: pickEvent(ev),
      items: [
        { ticketTypeId: first.id, name: first.name, quantity: 2, unitPriceCents: first.priceCents },
      ],
      totalCents: first.priceCents * 2,
      currency: "SGD",
      createdAt: new Date(now - 2 * 24 * H).toISOString(),
      holdExpiresAt: new Date(now - 2 * 24 * H + 600_000).toISOString(),
      holdSecondsLeft: 0,
      confirmedAt: new Date(now - 2 * 24 * H + 120_000).toISOString(),
      payment: {
        id: "pay_demo",
        status: "SUCCEEDED",
        amountCents: first.priceCents * 2,
        currency: "SGD",
      },
    },
  ];
  store = { events, tickets, reservations };
  return store;
}

const pickEvent = (e: EventDetail) => ({
  id: e.id,
  title: e.title,
  slug: e.slug,
  startsAt: e.startsAt,
  endsAt: e.endsAt,
});

const delay = (ms = 350) => new Promise((r) => setTimeout(r, ms));
const rid = () => `mock-${Math.random().toString(36).slice(2, 10)}`;
const fail = (
  status: number,
  message: string,
  extra: Partial<ConstructorParameters<typeof ApiError>[0]> = {},
) => new ApiError({ status, message, requestId: rid(), ...extra });

function b64url(o: object) {
  return btoa(JSON.stringify(o)).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function requireAuth() {
  if (!readTokens()) throw fail(401, "Unauthorized");
}

function refreshState(r: Store["reservations"][number]) {
  if (r.status === "HELD" || r.status === "PENDING_PAYMENT") {
    const left = r.holdExpiresAt
      ? Math.max(0, Math.floor((Date.parse(r.holdExpiresAt) - Date.now()) / 1000))
      : 0;
    r.holdSecondsLeft = left;
    if (r.status === "PENDING_PAYMENT" && r._pendingUntil && Date.now() > r._pendingUntil) {
      r.status = "CONFIRMED";
      r.confirmedAt = new Date().toISOString();
      if (r.payment) r.payment.status = "SUCCEEDED";
    } else if (left === 0 && r.status === "HELD") {
      r.status = "EXPIRED";
      releaseHold(r);
    }
  }
  return r;
}

function releaseHold(r: Reservation) {
  r.items.forEach((it) => {
    const t = db().tickets.find((x) => x.id === it.ticketTypeId);
    if (t) {
      t.remaining += it.quantity;
      t.held -= it.quantity;
    }
  });
}

const idem = new Map<string, { sig: string; id: string }>();
let loginAttempts = 0;

export const mockApi: EventHubApi = {
  async login(email, password) {
    await delay(500);
    loginAttempts++;
    if (loginAttempts > 8) throw fail(429, "Too many requests", { retryAfterSeconds: 30 });
    if (!email.includes("@"))
      throw fail(400, "Please check the highlighted fields.", {
        fieldErrors: { email: ["Enter a valid email"] },
      });
    if (password === "wrong") throw fail(401, "Invalid credentials");
    const payload = {
      sub: "usr_demo",
      tenantSlug: getTenantSlug(),
      role: "ATTENDEE",
      exp: Math.floor(Date.now() / 1000) + 3600 * 24 * 365,
    };
    return {
      accessToken: `${b64url({ alg: "none" })}.${b64url(payload)}.mock`,
      refreshToken: rid(),
    };
  },
  async logout(_refreshToken) {
    await delay(150);
  },
  // In preview, a link token "expired" is refused, and an invitation token "taken" names
  // an address that already has an account. Any other token works.
  async forgotPassword(_email) {
    await delay();
  },
  async resetPassword(token, password) {
    await delay();
    if (password.length < 8)
      throw fail(400, "Please check the highlighted fields.", {
        fieldErrors: { password: ["String must contain at least 8 character(s)"] },
      });
    if (token === "expired") throw fail(400, "invalid or expired reset token");
  },
  async register(token, password) {
    await delay();
    if (password.length < 8)
      throw fail(400, "Please check the highlighted fields.", {
        fieldErrors: { password: ["String must contain at least 8 character(s)"] },
      });
    if (token === "expired") throw fail(400, "invalid or expired invite");
    if (token === "taken") throw fail(409, "email already registered");
    return { id: "usr_invited", email: "new.member@example.com", role: "ATTENDEE" };
  },
  async listEvents(cursor) {
    requireAuth();
    await delay();
    // As the real list does with ?upcoming=true: nothing that has ended.
    const all = db()
      .events.filter((e) => e.status === "PUBLISHED" && Date.parse(e.endsAt) > Date.now())
      .sort((a, b) => a.startsAt.localeCompare(b.startsAt));
    const start = cursor ? Number(cursor) : 0;
    const data = all.slice(start, start + 20);
    const next = start + 20 < all.length ? String(start + 20) : null;
    return { data, nextCursor: next };
  },
  async getEvent(id) {
    requireAuth();
    await delay();
    const e = db().events.find((x) => x.id === id);
    if (!e) throw fail(404, "Not Found");
    return { ...e };
  },
  async listTicketTypes(eventId) {
    requireAuth();
    await delay(250);
    return {
      data: db()
        .tickets.filter((t) => t.eventId === eventId)
        .sort((a, b) => a.priceCents - b.priceCents)
        .map((t) => ({ ...t })),
    };
  },
  async createReservation(body, key) {
    requireAuth();
    await delay(500);
    const sig = JSON.stringify(body);
    const prev = idem.get(key);
    if (prev) {
      if (prev.sig !== sig) throw fail(422, "Idempotency-Key reused with a different request");
      const r = db().reservations.find((x) => x.id === prev.id)!;
      // A replay answers with the original hold, whatever has happened to it since.
      return {
        id: r.id,
        eventId: r.event.id,
        status: "HELD",
        expiresAt: new Date(Date.parse(r.createdAt) + 10 * 60_000).toISOString(),
        items: body.items,
      };
    }
    const e = db().events.find((x) => x.id === body.eventId);
    if (!e) throw fail(404, "Not Found");
    if (Date.parse(e.endsAt) < Date.now()) throw fail(409, "event is not on sale");
    const lines = body.items.map((it) => {
      const t = db().tickets.find((x) => x.id === it.ticketTypeId && x.eventId === e.id);
      if (!t)
        throw fail(400, "Please check the highlighted fields.", {
          fieldErrors: { items: ["Unknown ticket type"] },
        });
      if (t.remaining < it.quantity) throw fail(409, "not enough tickets remaining");
      return { t, q: it.quantity };
    });
    lines.forEach(({ t, q }) => {
      t.remaining -= q;
      t.held += q;
    });
    const now = Date.now();
    const holdExpiresAt = new Date(now + 10 * 60_000).toISOString();
    const total = lines.reduce((s, l) => s + l.t.priceCents * l.q, 0);
    const r: Store["reservations"][number] = {
      id: `res_${Math.random().toString(36).slice(2, 10)}`,
      status: "HELD",
      event: pickEvent(e),
      items: lines.map(({ t, q }) => ({
        ticketTypeId: t.id,
        name: t.name,
        quantity: q,
        unitPriceCents: t.priceCents,
      })),
      totalCents: total,
      currency: "SGD",
      createdAt: new Date(now).toISOString(),
      holdExpiresAt,
      holdSecondsLeft: 600,
      confirmedAt: null,
      payment: null,
    };
    db().reservations.unshift(r);
    idem.set(key, { sig, id: r.id });
    return {
      id: r.id,
      eventId: e.id,
      status: "HELD",
      expiresAt: holdExpiresAt,
      items: body.items,
    };
  },
  async getReservation(id) {
    requireAuth();
    await delay(250);
    const r = db().reservations.find((x) => x.id === id);
    if (!r) throw fail(404, "Not Found");
    const { _pendingUntil: _p, ...rest } = refreshState(r);
    return structuredClone(rest);
  },
  async payReservation(id) {
    requireAuth();
    await delay(1800);
    const r = db().reservations.find((x) => x.id === id);
    if (!r) throw fail(404, "Not Found");
    refreshState(r);
    if (r.status !== "HELD") throw fail(409, `reservation is ${r.status.toLowerCase()}`);
    const roll = Math.random();
    const payment = {
      id: `pay_${rid()}`,
      status: "PENDING" as const,
      amountCents: r.totalCents,
      currency: "SGD",
    };
    if (roll < 0.15) {
      r.payment = { ...payment, status: "FAILED" };
      throw fail(402, "payment was declined", {
        body: { status: "HELD", error: "payment was declined" },
      });
    }
    if (roll < 0.4) {
      r.status = "PENDING_PAYMENT";
      r.payment = payment;
      r._pendingUntil = Date.now() + 7000;
      return {
        httpStatus: 202,
        body: {
          status: "PENDING",
          paymentId: payment.id,
          message: "waiting for the bank to respond",
        },
      };
    }
    r.status = "CONFIRMED";
    r.confirmedAt = new Date().toISOString();
    r.payment = { ...payment, status: "SUCCEEDED" };
    return { httpStatus: 200, body: { status: "CONFIRMED", amountCents: r.totalCents } };
  },
  async listReservations({ status, cursor }) {
    requireAuth();
    await delay();
    const all = db()
      .reservations.map(refreshState)
      .filter((r) => !status || r.status === status)
      .sort((a, b) => b.createdAt.localeCompare(a.createdAt));
    const start = cursor ? Number(cursor) : 0;
    return {
      data: all.slice(start, start + 20).map(({ _pendingUntil: _p, ...r }) => structuredClone(r)),
      nextCursor: start + 20 < all.length ? String(start + 20) : null,
    };
  },
  // In preview there is one post, "post_demo"; any other id is not found, as a hidden,
  // deleted or private one is for the real API.
  async getPost(id) {
    requireAuth();
    await delay();
    if (id !== "post_demo") throw fail(404, "not found");
    return structuredClone(demoPost);
  },
  async getThread(postId) {
    requireAuth();
    await delay();
    if (postId !== "post_demo") throw fail(404, "not found");
    return {
      comments: structuredClone(demoComments),
      count: countComments(demoComments),
      truncated: false,
    };
  },
  async addComment(postId, body, parentId) {
    requireAuth();
    await delay();
    if (postId !== "post_demo") throw fail(404, "not found");
    const id = rid();
    const comment: Comment = {
      id,
      parentId: parentId ?? null,
      authorId: "usr_demo",
      body,
      deleted: false,
      depth: 1,
      reactionCount: 0,
      createdAt: new Date().toISOString(),
      replies: [],
    };
    const parent = parentId ? findComment(demoComments, parentId) : null;
    if (parentId && !parent) throw fail(404, "no such parent comment on this post");
    if (parent) {
      comment.depth = parent.depth + 1;
      parent.replies.push(comment);
    } else demoComments.push(comment);
    demoPost.commentCount += 1;
    return { id };
  },
};

const demoPost: Post = {
  id: "post_demo",
  spaceId: "space_demo",
  authorId: "usr_organiser",
  body: "Doors open at 6:30. Who's coming early for the soundcheck?",
  reactionCount: 4,
  commentCount: 2,
  createdAt: "2026-10-20T10:00:00.000Z",
  space: {
    id: "space_demo",
    slug: "jazz-night",
    name: "Harbourfront Jazz Night",
    visibility: "PUBLIC",
  },
  canReply: true,
};
const demoComments: Comment[] = [
  {
    id: "c1",
    parentId: null,
    authorId: "usr_a",
    body: "I'll be there by 6!",
    deleted: false,
    depth: 1,
    reactionCount: 1,
    createdAt: "2026-10-20T10:05:00.000Z",
    replies: [
      {
        id: "c2",
        parentId: "c1",
        authorId: "usr_demo",
        body: "Same, see you at the front.",
        deleted: false,
        depth: 2,
        reactionCount: 0,
        createdAt: "2026-10-20T10:07:00.000Z",
        replies: [],
      },
    ],
  },
];
function findComment(list: Comment[], id: string): Comment | null {
  for (const c of list) {
    if (c.id === id) return c;
    const below = findComment(c.replies, id);
    if (below) return below;
  }
  return null;
}
const countComments = (list: Comment[]): number =>
  list.reduce((n, c) => n + 1 + countComments(c.replies), 0);
