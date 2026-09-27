FROM golang:1.27 AS build

WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=unknown

RUN CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
      -o /notifier ./cmd/notifier


FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /notifier /notifier

USER nonroot:nonroot

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD ["/notifier", "healthcheck"]

ENTRYPOINT ["/notifier"]

EXPOSE 9090
