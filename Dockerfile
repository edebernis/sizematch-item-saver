FROM golang:1.13 as builder

WORKDIR /go/src/app
COPY . .

ARG GITHUB_TOKEN
ENV GITHUB_TOKEN=$GITHUB_TOKEN

ENV GOPRIVATE github.com/edebernis
RUN echo "machine github.com login edebernis password ${GITHUB_TOKEN}" > $HOME/.netrc

RUN GOOS=linux \
    CGO_ENABLED=0 \
    go build -a -installsuffix cgo -o sizematch-item-saver

# ---------------------------------------------------------------------
FROM alpine:3.11
COPY --from=builder /go/src/app/sizematch-item-saver /

ENTRYPOINT ["/sizematch-item-saver"]
