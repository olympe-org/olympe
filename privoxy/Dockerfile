FROM debian:bookworm-slim

# Pont HTTP -> SOCKS5, pour permettre à ffmpeg (qui ne sait pas parler SOCKS5)
# de passer par le même proxy résidentiel (Raspberry Pi) que yt-dlp.
RUN apt-get update && apt-get install -y --no-install-recommends privoxy \
  && rm -rf /var/lib/apt/lists/*

COPY config.template /etc/privoxy/config.template
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8118

CMD ["/entrypoint.sh"]
