#!/bin/sh
set -e

# YTDLP_PROXY est de la forme "socks5://host:port" ; privoxy attend juste "host:port"
UPSTREAM="${YTDLP_PROXY#socks5://}"

if [ -z "$UPSTREAM" ]; then
  echo "[privoxy] YTDLP_PROXY n'est pas défini, rien à relayer." >&2
  exit 1
fi

sed "s|__UPSTREAM__|${UPSTREAM}|" /etc/privoxy/config.template > /etc/privoxy/config

exec privoxy --no-daemon /etc/privoxy/config
