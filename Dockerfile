# syntax=docker/dockerfile:1

# One image for every service in the monorepo.
#
# All four services and the setup step run from this same image with different
# commands, so `docker compose up` builds once rather than four times. They share
# a lockfile and most of their dependencies; building four near-identical images
# would spend four times the work to produce four nearly identical things.
#
# WHY DEV DEPENDENCIES STAY IN. The obvious second stage -- reinstall with
# --prod and copy the build in -- cannot stand this system up. The setup step
# needs the Prisma CLI to migrate, and scripts/ needs kafkajs, pg and avsc to
# create the topics and register the Avro schemas, and all of those are dev
# dependencies. A production image would be smaller and unable to start. This is
# a local development image and says so; a deployment image would be per-service,
# --prod, and would take its migrations and topics from a separate job.
FROM node:26-bookworm-slim AS build

# curl for the healthchecks compose defines against these containers, and
# openssl because Prisma's CLI expects it present.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl openssl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# pnpm, at exactly the version package.json's packageManager field names.
#
# NOT via corepack. `corepack enable` exits 127 on this base image -- corepack is
# no longer shipped with Node, and every guide written before that still opens
# with it. Installing the pinned version directly is one less moving part than
# installing corepack in order to install pnpm, and it fails loudly if the
# version stops existing rather than resolving to whatever is newest.
RUN npm install -g pnpm@9.15.0

# NOT ROOT FROM HERE DOWN.
#
# The node images ship an unprivileged `node` user and default to running as
# root anyway, so a Dockerfile that never says otherwise ships four services
# running as uid 0. Nothing here needs it: they open a listening socket above
# 1024, read their own code, and talk to Postgres and Kafka.
#
# The switch happens BEFORE the install rather than after the build, with a
# chown on the still-empty directory. Doing it the other way -- build as root,
# then `chown -R node:node /app` at the end -- also works, but changing a file's
# ownership copies that file into the new layer, so a recursive chown over a
# fully installed node_modules writes a layer containing the whole tree again.
# Ordering it this way means no file is ever owned by the wrong user.
#
# services/auth/keys is created here, empty and owned by node, because Docker
# seeds a fresh named volume from the image's directory AND ITS OWNERSHIP. The
# directory does not otherwise exist in the image (.dockerignore drops **/keys,
# deliberately -- a signing key must never be in a layer), so without this the
# volume would be created root-owned and the setup container could not write
# the keypair into it.
RUN mkdir -p /app/services/auth/keys && chown -R node:node /app
USER node

WORKDIR /app

# Manifests first, so a source-only change does not re-run the install. Every
# workspace package's package.json is needed for the install to resolve the
# workspace: protocol, which is why this is a list rather than one COPY.
COPY --chown=node:node package.json pnpm-lock.yaml pnpm-workspace.yaml turbo.json ./
COPY --chown=node:node contracts/package.json ./contracts/
COPY --chown=node:node packages/auth-middleware/package.json ./packages/auth-middleware/
COPY --chown=node:node packages/config/package.json ./packages/config/
COPY --chown=node:node packages/logger/package.json ./packages/logger/
COPY --chown=node:node packages/tsconfig/package.json ./packages/tsconfig/
COPY --chown=node:node packages/types/package.json ./packages/types/
COPY --chown=node:node services/auth/package.json ./services/auth/
COPY --chown=node:node services/event/package.json ./services/event/
COPY --chown=node:node services/live/package.json ./services/live/
COPY --chown=node:node services/relay/package.json ./services/relay/
COPY --chown=node:node services/search/package.json ./services/search/
COPY --chown=node:node services/ticketing/package.json ./services/ticketing/

# --frozen-lockfile: the build fails rather than quietly resolving something the
# lockfile does not describe. An image that installed a different tree than the
# one CI tested is an image whose test results mean nothing.
RUN pnpm install --frozen-lockfile

COPY --chown=node:node . .

# Each service's build runs `prisma generate` before tsc. That works without a
# reachable database -- generate reads the schema file, it does not introspect --
# and with @prisma/adapter-pg there is no platform-specific query engine to fetch,
# so the generated client is portable. That is the whole reason this image does
# not need binaryTargets.
RUN pnpm run build

# No CMD. Every compose service supplies its own command, and a default here
# would be a guess about which of five things this image is for.
