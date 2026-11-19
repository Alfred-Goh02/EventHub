import "./lib/error-capture";

import { consumeLastCapturedError } from "./lib/error-capture";
import { renderErrorPage } from "./lib/error-page";

type ServerEntry = {
  fetch: (request: Request, env: unknown, ctx: unknown) => Promise<Response> | Response;
};

let serverEntryPromise: Promise<ServerEntry> | undefined;

async function getServerEntry(): Promise<ServerEntry> {
  if (!serverEntryPromise) {
    serverEntryPromise = import("@tanstack/react-start/server-entry").then(
      (m) => (m.default ?? m) as ServerEntry,
    );
  }
  return serverEntryPromise;
}

// h3 swallows in-handler throws into a normal 500 Response with body
// {"unhandled":true,"message":"HTTPError"} — try/catch alone never fires for those.
async function normalizeCatastrophicSsrResponse(response: Response): Promise<Response> {
  if (response.status < 500) return response;
  const contentType = response.headers.get("content-type") ?? "";
  if (!contentType.includes("application/json")) return response;

  const body = await response.clone().text();
  if (!isH3SwallowedErrorBody(body)) return response;

  console.error(consumeLastCapturedError() ?? new Error(`h3 swallowed SSR error: ${body}`));
  return new Response(renderErrorPage(), {
    status: 500,
    headers: { "content-type": "text/html; charset=utf-8" },
  });
}

function isH3SwallowedErrorBody(body: string): boolean {
  try {
    const payload = JSON.parse(body) as { unhandled?: unknown; message?: unknown };
    return payload.unhandled === true && payload.message === "HTTPError";
  } catch {
    return false;
  }
}

/**
 * THE DEPLOYMENT'S OWN DOMAIN, from APP_URL_TEMPLATE -- the address Auth and Notification
 * build their links on, https://{tenant}.example.org -- or null where it is not set: a
 * laptop, Lovable's preview, a static host. The browser uses the real API only on a tenant
 * of this domain or of eventhub.localhost (api/env.ts), and the sample data anywhere else.
 *
 * Set and not an address, every page answers 500, with the reason in the log, rather than
 * serve on the sample data: on a real host, that would look like a working site with nobody's
 * events. nitro loads this module on the first request, not at start, so the throw lands
 * there; the page is infra/web's readiness probe, so such a pod is never ready (measured:
 * favicon.ico 200, / and /config.js 500).
 */
function publicDomainOf(template: string | undefined): string | null {
  if (!template) return null;
  const domain = /^https?:\/\/\{tenant\}\.([a-z0-9.-]+)(?::[1-9][0-9]{0,4})?\/?$/.exec(
    template,
  )?.[1];
  const label = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;
  const labels = domain?.split(".") ?? [];
  if (domain === undefined || labels.length < 2 || !labels.every((l) => label.test(l))) {
    throw new Error(
      `APP_URL_TEMPLATE must be http(s)://{tenant}.<domain>[:port] with a lower-case domain (got ${JSON.stringify(template)})`,
    );
  }
  return domain;
}

const runtimeEnv =
  (globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env ?? {};
const PUBLIC_DOMAIN = publicDomainOf(runtimeEnv.APP_URL_TEMPLATE);

/**
 * /config.js: the domain, as a script the page loads before the app (routes/__root.tsx), so
 * the API client is chosen with it from the first line that runs. A script of its own rather
 * than inline in the page: the shell is rendered on the server and hydrated in the browser,
 * where the environment is not, and the two must agree. The domain passed the check above,
 * and JSON.stringify writes it as a string, so nothing in it runs.
 */
function configScript(): Response {
  return new Response(`window.__EVENTHUB__ = ${JSON.stringify({ domain: PUBLIC_DOMAIN })};\n`, {
    status: 200,
    headers: {
      "content-type": "text/javascript; charset=utf-8",
      "cache-control": "no-store",
      "x-content-type-options": "nosniff",
    },
  });
}

export default {
  async fetch(request: Request, env: unknown, ctx: unknown) {
    if (
      new URL(request.url).pathname === "/config.js" &&
      (request.method === "GET" || request.method === "HEAD")
    ) {
      return configScript();
    }
    try {
      const handler = await getServerEntry();
      const response = await handler.fetch(request, env, ctx);
      return await normalizeCatastrophicSsrResponse(response);
    } catch (error) {
      console.error(error);
      return new Response(renderErrorPage(), {
        status: 500,
        headers: { "content-type": "text/html; charset=utf-8" },
      });
    }
  },
};
