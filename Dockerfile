FROM golang:1.25.6-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -o login-tracer .

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/login-tracer .

CMD ["./login-tracer"]
