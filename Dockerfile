# ---------- stage: base (install deps) ----------
FROM oven/bun:1 AS base
WORKDIR /usr/src/app

COPY package.json bun.lock ./
RUN bun install --frozen-lockfile

# ---------- stage: build ----------
FROM base AS build
COPY . .
RUN bun run build

# ---------- stage: final (runtime with Docker CLI + git) ----------
FROM oven/bun:1 AS final
WORKDIR /usr/src/app

# Install Docker CLI (needed to spawn job containers via host socket)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      docker.io \
      git \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=base /usr/src/app/node_modules ./node_modules
COPY --from=build /usr/src/app/dist ./dist
COPY package.json ./

ENV NODE_ENV=production
ENV HOME=/root

CMD ["bun", "run", "dist/main.js"]
