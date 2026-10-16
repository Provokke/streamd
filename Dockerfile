FROM golang:1.24 AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/ ./cmd/...

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/streamd /out/streamctl /usr/local/bin/
VOLUME /data
EXPOSE 7070
ENTRYPOINT ["streamd", "-data", "/data", "-addr", ":7070"]
