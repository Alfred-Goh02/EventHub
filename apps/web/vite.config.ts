// @lovable.dev/vite-tanstack-config already includes the following — do NOT add them manually
// or the app will break with duplicate plugins:
//   - TanStack devtools (dev-only, first), tanstackStart, viteReact, tailwindcss, tsConfigPaths,
//     nitro (build-only using cloudflare as a default target), VITE_* env injection, @ path alias,
//     React/TanStack dedupe, error logger plugins, and sandbox detection (port/host/strictPort).
// You can pass additional config via defineConfig({ vite: { ... }, etc... }) if needed.
import { defineConfig } from "@lovable.dev/vite-tanstack-config";

export default defineConfig({
  tanstackStart: {
    // Redirect TanStack Start's bundled server entry to src/server.ts (our SSR error wrapper).
    // nitro/vite builds from this
    server: { entry: "server" },
  },
  vite: {
    // Outside Lovable's sandbox the shared config listens on "::" -- every interface, so the
    // whole LAN could reach the dev server -- on port 8080. Loopback only, as every other port
    // in this repository is (#151), on the port the gateway's CORS rule and the README expect.
    // The browser reaches it as http://<tenant>.eventhub.localhost:5173, which resolves here.
    server: { host: "127.0.0.1", port: 5173, strictPort: true },
  },
});
