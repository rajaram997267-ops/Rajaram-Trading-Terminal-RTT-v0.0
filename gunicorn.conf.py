# Picked up automatically by gunicorn (no Start Command change needed on
# Render - it just runs "gunicorn main:app" and gunicorn looks for this
# file in the working directory on its own).
#
# WHY THIS FILE EXISTS: gunicorn's default request timeout is 30 seconds -
# a worker that hasn't responded within that window gets assumed hung and
# is force-killed (SIGKILL), then a fresh one is booted. On a cold start
# (right after a deploy, or Render waking a spun-down free instance), the
# very first request that needs to resolve an option contract triggers
# _load_instrument_master() in main.py - which downloads and parses NSE's
# entire instrument file in TWO passes (deliberate: trades time for lower
# peak memory, see that function's docstring). On a throttled free-tier
# cold-start network, that combined work can genuinely take longer than
# 30 seconds, which is exactly what killed a worker on 2026-08-21 (see
# Render's Events/Logs: "WORKER TIMEOUT" immediately followed by
# "SIGKILL"). It self-healed (gunicorn restarted a fresh worker
# automatically), but a longer timeout avoids the kill in the first place.
#
# This does NOT add threading/concurrency (no gthread, no SSE-style
# streaming endpoints in this codebase) - just a longer patience window
# for the one genuinely slow request path that exists today.

workers = 1
timeout = 90   # was gunicorn's default of 30 - the instrument-file cold load can exceed that
