# syntax=docker/dockerfile:1
FROM golang:1.27.1-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/wagering ./cmd/wagering \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/migrate ./cmd/migrate

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/wagering /wagering
COPY --from=build /out/migrate /migrate
USER nonroot:nonroot
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=4s --retries=12 CMD ["/wagering", "healthcheck"]
ENTRYPOINT ["/wagering"]
