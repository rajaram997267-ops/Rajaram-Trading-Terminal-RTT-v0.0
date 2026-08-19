# Picked up automatically by gunicorn (no Start Command change needed on
# Render - it just runs "gunicorn main:app" and gunicorn looks for this
# file in the working directory on its own).
#
# WHY THIS FILE EXISTS: the default "sync" worker class can only handle
# ONE request at a time, ever - it's not concurrent within a worker. The
# /api/stream/alerts SSE endpoint holds a connection open indefinitely
# (that's the whole point of SSE), so with a sync worker, one open
# browser tab permanently occupies the entire worker. Every other
# request - the settings page, paper trading page, even Render's own
# health check - has nowhere to go and 503s, and Render ends up
# restarting the service on a loop when its health check can't get
# through. "gthread" with multiple threads lets one worker process serve
# several requests concurrently, so a long-lived SSE connection no
# longer blocks anything else.

worker_class = "gthread"
threads = 8       # comfortably covers a handful of open dashboard tabs + normal traffic
workers = 1        # free-tier RAM is tight - stick with 1 process, threads cover the concurrency
timeout = 30       # request timeout for NON-streaming requests; SSE's own long-poll loop isn't affected by this
