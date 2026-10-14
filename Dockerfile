FROM --platform=${BUILDPLATFORM} docker.io/library/golang:1.26 AS build
ARG TARGETOS TARGETARCH
WORKDIR /workspace
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY main.go ./
COPY internal/ internal/
RUN --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg/mod \
    GOOS=${TARGETOS} GOARCH=${TARGETARCH} CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o extension .

FROM gcr.io/distroless/static:nonroot
COPY --from=build /workspace/extension /extension
USER 65532:65532
ENTRYPOINT ["/extension"]
