#!/usr/bin/env bash
# Local end-to-end smoke test for the running stack (started with `make up`).
# Exits non-zero on the first failed check so it can gate CI.
set -euo pipefail

API="${API:-http://localhost:8080}"
pass() { printf "  PASS  %s\n" "$1"; }
fail() { printf "  FAIL  %s\n" "$1"; exit 1; }

echo "== Waiting for API to be ready =="
for i in $(seq 1 30); do
  curl -sf "$API/readyz" >/dev/null 2>&1 && break
  [ "$i" = 30 ] && fail "API not ready after 30s"; sleep 1
done
pass "API /readyz is ready"

echo "== Waiting for telemetry to arrive =="
for i in $(seq 1 30); do
  N=$(curl -sf "$API/api/v1/gpus" | python3 -c 'import sys,json;print(len(json.load(sys.stdin)["items"]))')
  [ "$N" -gt 0 ] && break
  [ "$i" = 30 ] && fail "no GPUs after 30s"; sleep 1
done

# All functional assertions run in one Python process against the live API.
python3 - "$API" <<'PY'
import sys, json, datetime, urllib.request, urllib.error
API = sys.argv[1] + "/api/v1"
def get(path):
    with urllib.request.urlopen(API + path) as r: return r.status, json.load(r)
def code(path):
    try:
        with urllib.request.urlopen(API + path) as r: return r.status
    except urllib.error.HTTPError as e: return e.code
def check(ok, msg): print(("  PASS  " if ok else "  FAIL  ") + msg); (None if ok else sys.exit(1))

_, gpus = get("/gpus")
check(len(gpus["items"]) > 0, f"GET /gpus returns {len(gpus['items'])} GPUs")
gpu = gpus["items"][0]["uuid"]
check(all(k in gpus["items"][0] for k in ("uuid","gpu_id","hostname")), "GPU objects have uuid/gpu_id/hostname")

_, t = get(f"/gpus/{gpu}/telemetry?limit=500")
ts = [i["timestamp"] for i in t["items"]]
check(len(ts) > 0, f"telemetry returns {len(ts)} points")
check(ts == sorted(ts), "telemetry is ordered by time")

_, m = get(f"/gpus/{gpu}/telemetry?metric=DCGM_FI_DEV_GPU_TEMP&limit=200")
check(all(i["metric"] == "DCGM_FI_DEV_GPU_TEMP" for i in m["items"]), "metric filter returns only that metric")

mid = t["items"][len(t["items"])//2]["timestamp"]
_, w = get(f"/gpus/{gpu}/telemetry?start_time={mid}&limit=500")
check(w["items"][0]["timestamp"] == mid and all(i["timestamp"] >= mid for i in w["items"]), "start_time is inclusive")
_, w2 = get(f"/gpus/{gpu}/telemetry?end_time={mid}&limit=500")
check(w2["items"][-1]["timestamp"] == mid and all(i["timestamp"] <= mid for i in w2["items"]), "end_time is inclusive")

_, p1 = get(f"/gpus/{gpu}/telemetry?limit=10")
_, p2 = get(f"/gpus/{gpu}/telemetry?limit=10&cursor={p1['next_cursor']}")
a = [i["timestamp"] for i in p1["items"]]; b = [i["timestamp"] for i in p2["items"]]
check(set(a).isdisjoint(b) and b[0] >= a[-1], "cursor pagination continues without overlap")

year = datetime.datetime.fromisoformat(ts[0].replace("Z","+00:00")).year
check(year >= 2026, f"timestamp is processing time (year {year}, not the CSV's 2025)")

check(code("/gpus/GPU-missing/telemetry") == 404, "unknown GPU -> 404")
check(code(f"/gpus/{gpu}/telemetry?start_time=nope") == 400, "bad start_time -> 400")
check(code(f"/gpus/{gpu}/telemetry?limit=0") == 400, "bad limit -> 400")
print("\nAll functional checks passed.")
PY
