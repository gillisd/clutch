1. ~~clutch incapable of restarting connection if upstream restarts~~ Fixed: `WithDialer` option enables automatic reconnection with exponential backoff
2. ~~clutch errors on wss:// endpoint~~ Fixed: clutchpedal uses custom `websocket.Dialer` with `--insecure` flag for TLS config
