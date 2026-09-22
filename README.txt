GALAXI V28 POSITIONS DIRECT

Base: V27.

Fix principal:
- V27's flat-account fallback referenced market out of scope, so it could not create the simple entry.
- V28 passes the analyzed market into executeDecision.
- If flat and AI returns no OPEN action, V28 selects the first valid market with directional/momentum evidence; if none qualifies, it uses the first valid non-cooldown market.
- Minimum expected net default reduced to 0.01% to avoid paralysis.
- PAPER remains default.

Do not enable LIVE for this test.
