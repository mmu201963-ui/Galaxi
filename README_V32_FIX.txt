GALAXI V32 - FIX WEB SERVER

This package fixes the Railway "Application failed to respond" problem.

The V32 trading engine was starting, but it did not create an HTTP listener on Railway's PORT. Railway therefore could not reach the service even though Binance WebSocket was connected.

This version adds:
- HTTP server bound to 0.0.0.0 and process.env.PORT
- / health endpoint
- /state JSON endpoint
- simple live dashboard at /
- trading/AI/behavioral engine unchanged

Default remains PAPER. Do not enable LIVE until the PAPER flow has been verified.
