# Multi-Stream Pocket Option Dashboard

Two Railway services stream 16 assets across 6 SSIDs (3 per VM).

## Instances

- **VM A (demo)** — MARA_otc, GME_otc, PLTR_otc, EURRUB_otc, LINK_otc, SOL-USD_otc, MATIC_otc, TON-USD_otc
- **VM B (live)** — UKBrent_otc, USCrude_otc, JPN225_otc, SP500_otc, XOM_otc, XAGUSD_otc, XAUUSD_otc, XNGUSD_otc

## Env vars (per Railway service)

### Service A (VM_ROLE=A)
- `VM_ROLE=A`
- `STARTUP_DELAY_SECS=0`
- `SSID_VM1_SLOT1`, `SSID_VM1_SLOT2`, `SSID_VM1_SLOT3`

### Service B (VM_ROLE=B)
- `VM_ROLE=B`
- `STARTUP_DELAY_SECS=60` (stagger to avoid IP-level auth burst)
- `SSID_VM2_SLOT1`, `SSID_VM2_SLOT2`, `SSID_VM2_SLOT3`

## Endpoints

- `/` — dashboard (all assets, all slots)
- `/api/candles` — full snapshot with indicators
- `/api/perf` — live performance stats
- `/api/perf/history` — last ~2h of samples
- `/api/health` — liveness
