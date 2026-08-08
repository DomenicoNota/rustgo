FROM golang:1.26.5-alpine AS build
WORKDIR /src/backend
COPY backend/go.mod backend/go.sum* ./
RUN go mod download
COPY backend/ ./
ARG SERVICE=api
RUN CGO_ENABLED=0 go build -trimpath -o /out/logstream ./cmd/${SERVICE}

FROM alpine:3.24.1
WORKDIR /app
RUN apk add --no-cache ca-certificates \
    && addgroup -S logstream \
    && adduser -S -G logstream -h /app logstream
COPY --from=build --chown=logstream:logstream /out/logstream /app/logstream
COPY --chown=logstream:logstream db/migrations /migrations
ENV MIGRATIONS_DIR=/migrations
EXPOSE 8080 9091
USER logstream
CMD ["/app/logstream"]
