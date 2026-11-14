# syntax=docker/dockerfile:1

# The web app's image, built from apps/web alone (`docker build apps/web`).
#
# ITS OWN IMAGE, NOT THE BACKEND'S. The web app is its own pnpm project, outside the
# workspace (README.md): it imports nothing from the services, and its six hundred packages
# have no place in the image every service runs. The backend image is ~1.1 GB because it
# keeps dev dependencies for setup; this one ships only nitro's output, which bundles what
# the server needs.
#
# The same base image as the backend's Dockerfile, at the Node major .nvmrc names, and the
# same pnpm, at the version the root package.json's packageManager field names.
FROM node:26-bookworm-slim AS build
RUN npm install -g pnpm@9.15.0

# Not root, from the start: the install and the build need no privilege, and nothing is ever
# owned by the wrong user (the backend's Dockerfile says why the order matters).
RUN mkdir /web && chown node:node /web
USER node
WORKDIR /web

COPY --chown=node:node package.json pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile --ignore-workspace

COPY --chown=node:node . .
RUN pnpm run build

# ---------------------------------------------------------------------------------------
# What runs: nitro's node-server output and nothing else. No pnpm, no source, no
# node_modules beyond the ones nitro copied into .output/server.
FROM node:26-bookworm-slim
USER node
WORKDIR /web
COPY --from=build --chown=node:node /web/.output ./.output

# nitro's node-server reads PORT and HOST. Every interface inside the container: the pod's
# own address is how the kubelet probes it and the Service reaches it.
ENV NODE_ENV=production PORT=3000 HOST=0.0.0.0
EXPOSE 3000
CMD ["node", ".output/server/index.mjs"]
