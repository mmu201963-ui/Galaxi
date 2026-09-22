GALAXI V22 FINAL FIX

Critical fix:
- PAPER refreshes Binance prices via REST on every cycle.
- PnL/current price can no longer remain frozen at entry merely because the WebSocket tick stream is stale.
- PAPER TP/SL and time-based closes continue to run from the refreshed mark.

Deployment:
1. Replace the repository's index.js with this index.js.
2. Keep PAPER while validating movement and closes.
3. Restart/redeploy Railway.
4. Verify that position "actual" prices change and PnL moves.
5. Only after that consider LIVE.

Do not place API keys in this file or commit them to GitHub. Use Railway Variables/Secrets.
