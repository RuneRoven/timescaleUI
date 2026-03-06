# Stage 1: Build Tailwind CSS
FROM node:22-alpine AS tailwind
WORKDIR /build
COPY tailwind.config.js package.json* ./
RUN npm install -D tailwindcss@3
COPY web/ web/
RUN npx tailwindcss -i web/static/css/tailwind-input.css -o web/static/css/app.css --minify

# Stage 2: Build Go binary
FROM golang:1.26-alpine AS builder
RUN apk add --no-cache git ca-certificates
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=tailwind /build/web/static/css/app.css web/static/css/app.css
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /tsui ./cmd/tsui

# Stage 3: Final distroless image
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /tsui /tsui
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
VOLUME /data
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/tsui"]
