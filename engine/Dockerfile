# Build Stage
FROM golang:alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go mod tidy
RUN go build -o sankofa ./cmd/sankofa/main.go

# Runtime Stage
FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/sankofa .
# Copy .env.example as .env for default config (user can override with volume or env vars)
COPY .env.example .env

EXPOSE 3000

CMD ["./sankofa"]
