# Multi-stage: build CRA, serve with nginx. Push the resulting image to Amazon ECR.
#
# Build (set API env for compile-time embedding):
#   docker build -t ai-gateway-client \
#     --build-arg REACT_APP_DISCOVERY_URL= \
#     --build-arg REACT_APP_LEADER_FALLBACK_URL=http://YOUR_LEADER:8000 \
#     .
#
# Run locally:
#   docker run --rm -p 8080:80 ai-gateway-client

FROM node:22-alpine AS build
WORKDIR /app

COPY package.json ./
RUN npm install

COPY . .

ARG REACT_APP_DISCOVERY_URL=
ARG REACT_APP_LEADER_FALLBACK_URL=http://localhost:8000
ARG REACT_APP_LEADER_IP=
ENV REACT_APP_DISCOVERY_URL=$REACT_APP_DISCOVERY_URL
ENV REACT_APP_LEADER_FALLBACK_URL=$REACT_APP_LEADER_FALLBACK_URL
ENV REACT_APP_LEADER_IP=$REACT_APP_LEADER_IP

RUN npm run build

FROM nginx:1.27-alpine
COPY deploy/nginx/docker-default.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/build /usr/share/nginx/html
EXPOSE 80
