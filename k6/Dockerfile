#FROM golang:1.22 as builder
#
#ENV Xk6_VERSION=v0.12.0
#ENV XK6_KAFKA_VERSION=latest
#
#RUN go install go.k6.io/xk6/cmd/xk6@${Xk6_VERSION}
#
#RUN xk6 build \
#    --with github.com/mostafa/xk6-kafka@${XK6_KAFKA_VERSION}
#
#FROM alpine:3.19
#
#COPY --from=builder /go/k6 /usr/bin/k6
#
#ENTRYPOINT ["k6"]


#FROM grafana/k6:latest
#
#USER root
#RUN apk add --no-cache git build-base
#RUN go install go.k6.io/xk6/cmd/xk6@latest
#RUN /root/go/bin/xk6 build --with github.com/mostafa/xk6-kafka@latest -o /usr/bin/k6
#
#USER k6
#WORKDIR /scripts