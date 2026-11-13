import "@testing-library/jest-dom/vitest";

// Node 26 has a localStorage global of its own, undefined unless Node is given
// --localstorage-file, and it shadows jsdom's. The session lives in localStorage.
if (typeof globalThis.localStorage?.setItem !== "function") {
  const items = new Map<string, string>();
  const storage: Storage = {
    get length() {
      return items.size;
    },
    clear: () => items.clear(),
    getItem: (k) => items.get(k) ?? null,
    key: (i) => [...items.keys()][i] ?? null,
    removeItem: (k) => {
      items.delete(k);
    },
    setItem: (k, v) => {
      items.set(k, String(v));
    },
  };
  Object.defineProperty(globalThis, "localStorage", { configurable: true, value: storage });
}

Object.defineProperty(window, "scrollTo", {
  writable: true,
  value: () => {},
});

Object.defineProperty(window, "matchMedia", {
  writable: true,
  value: (query: string) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: () => {},
    removeListener: () => {},
    addEventListener: () => {},
    removeEventListener: () => {},
    dispatchEvent: () => {},
  }),
});
