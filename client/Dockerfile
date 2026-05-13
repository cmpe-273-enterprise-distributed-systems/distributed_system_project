# Multi-stage: build CRA, serve with nginx. Push the resulting image to Amazon ECR.
#
# Build — bake leader discovery + fallback (CRA reads env at build time only):
#   docker build -t ai-gateway-client \
#     --build-arg REACT_APP_UPSTASH_REDIS_REST_URL="https://....upstash.io" \
#     --build-arg REACT_APP_UPSTASH_REDIS_REST_READ_ONLY_TOKEN="..." \
#     --build-arg REACT_APP_LEADER_FALLBACK_URL="http://YOUR_LEADER_PUBLIC:8000" \
#     .
#
# Upstash: Database → REST API → Allowed Origins must include your UI origin
# (e.g. http://localhost:3000 and http://YOUR_EC2_IP).
#
# Run locally:
#   docker run --rm -p 8080:80 ai-gateway-client

FROM node:22-alpine AS build
WORKDIR /app

COPY package.json ./
RUN npm install

COPY . .

ARG REACT_APP_UPSTASH_REDIS_REST_URL=
ARG REACT_APP_UPSTASH_REDIS_REST_READ_ONLY_TOKEN=
ARG REACT_APP_LEADER_FALLBACK_URL=http://localhost:8000
ARG REACT_APP_LEADER_IP=
ENV REACT_APP_UPSTASH_REDIS_REST_URL=$REACT_APP_UPSTASH_REDIS_REST_URL
ENV REACT_APP_UPSTASH_REDIS_REST_READ_ONLY_TOKEN=$REACT_APP_UPSTASH_REDIS_REST_READ_ONLY_TOKEN
ENV REACT_APP_LEADER_FALLBACK_URL=$REACT_APP_LEADER_FALLBACK_URL
ENV REACT_APP_LEADER_IP=$REACT_APP_LEADER_IP

RUN npm run build

FROM nginx:1.27-alpine
COPY deploy/nginx/docker-default.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/build /usr/share/nginx/html
EXPOSE 80
