FROM alpine:3.20

RUN apk add --no-cache iptables iproute2 bash
COPY testbed/docker/router/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
