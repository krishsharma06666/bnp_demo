"""Ride Booking API v1 - the front door for riders and drivers. Owner: Person 3.

Runs as 3 copies behind the HAProxy load balancer (http://localhost:8000). Swagger: /docs.
Every write goes to Kafka and returns 202 in milliseconds; the API never writes to the database.

RIDER   (X-API-Key: RIDER_API_KEY)
  POST /v1/bookings                          book a ride            -> 202 {booking_id, fare, surge}
  GET  /v1/bookings/{id}                     ride status, driver, payment (read from DB)
  GET  /v1/bookings/{id}/stream?key=...      live updates (Server-Sent Events)
  POST /v1/bookings/{id}/cancel              cancel
DRIVER  (X-API-Key: DRIVER_API_KEY)
  POST /v1/drivers/{driver_id}/respond       {"booking_id": 1, "action": "ACCEPT" | "REJECT"}
  POST /v1/drivers/{driver_id}/trips/{id}/start
  POST /v1/drivers/{driver_id}/trips/{id}/complete
PUBLIC
  GET /health  /ready  /metrics  /v1/surge  /live (live ride tracker page)

Per booking request: API key -> rate limit (429) -> Idempotency-Key (200 same answer / 409 in progress)
-> shape check (422) -> booking_id + fare x surge -> Kafka acks=all (503 if not confirmed) -> 202.
"""
import asyncio
import json
import os
import secrets
import socket
import time
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from prometheus_client import make_asgi_app
from pydantic import BaseModel, Field

from common import config
from common.events import Envelope, booking_raw_event
from common.geo import AREAS, base_fare
from common.logger import get_logger
from common.metrics import API_LATENCY, API_REQUESTS, SURGE
from common.topics import BOOKING_RAW, DRIVER_RESPONSE, TRIP_UPDATES
from services.api.idempotency import IdempotencyStore
from services.api.limits import BookingIdGenerator, NodeLease, TokenBucket
from services.api.reader import get_ride, is_final
from services.api.surge import SurgeEngine

log = get_logger("booking-api")
IST = ZoneInfo("Asia/Kolkata")
REPLICA = socket.gethostname()
RATE = float(os.getenv("RATE_LIMIT_PER_CLIENT", "20"))
BURST = int(os.getenv("RATE_LIMIT_BURST", "40"))
MAX_BODY = 10_000


class BookingRequest(BaseModel):
    user_id: int = Field(ge=1)
    pickup_location: str = Field(min_length=1, max_length=64)
    destination_location: str = Field(min_length=1, max_length=64)


class DriverAnswer(BaseModel):
    booking_id: int = Field(ge=1)
    action: Literal["ACCEPT", "REJECT"]


def csv_time(ts: datetime) -> str:
    return f"{ts:%d-%m-%Y} {ts.hour}.{ts:%M}"       # same format as bookings.csv (the validator parses it)


def _key_ok(given: str | None, *allowed: str) -> bool:
    return given is not None and any(secrets.compare_digest(given, k) for k in allowed if k)


def rider(x_api_key: str | None = Header(None)):
    if not _key_ok(x_api_key, config.RIDER_API_KEY, config.ADMIN_API_KEY):
        raise HTTPException(401, "missing or wrong X-API-Key (rider)")


def driver(x_api_key: str | None = Header(None)):
    if not _key_ok(x_api_key, config.DRIVER_API_KEY, config.ADMIN_API_KEY):
        raise HTTPException(401, "missing or wrong X-API-Key (driver)")


def create_app(sender=None, redis_client=None, db_reader=None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if app.state.sender is None:
            from services.api.kafka_sender import KafkaSender
            app.state.sender = KafkaSender()
        if app.state.redis is None:
            try:
                import redis
                app.state.redis = redis.Redis.from_url(config.REDIS_URL, socket_timeout=0.3,
                                                       socket_connect_timeout=0.3)
            except Exception:
                app.state.redis = None
            app.state.idem = IdempotencyStore(app.state.redis, log)
            app.state.surge = SurgeEngine(app.state.redis)
            app.state.lease = NodeLease(app.state.redis, REPLICA, log)
            app.state.ids = BookingIdGenerator(app.state.lease.node_id)

        async def keep_lease():
            while True:
                await asyncio.sleep(60)
                await asyncio.to_thread(app.state.lease.renew)
        renewer = asyncio.create_task(keep_lease())
        if app.state.reader is None:
            from common.db import pool

            def reader(booking_id):
                with pool(config.DATABASE_URL).connection() as c:
                    return get_ride(c, booking_id)
            app.state.reader = reader
        log.info("started", replica=REPLICA, node_id=app.state.ids.node_id)
        yield
        renewer.cancel()
        app.state.lease.release()
        app.state.sender.close()

    app = FastAPI(title="Ride Booking API", version="1.0", lifespan=lifespan)
    app.state.sender, app.state.redis, app.state.reader = sender, redis_client, db_reader
    app.state.idem = IdempotencyStore(redis_client, log)
    app.state.surge = SurgeEngine(redis_client)
    app.state.limiter = TokenBucket(RATE, BURST)
    app.state.lease = NodeLease(redis_client, REPLICA, log if redis_client is not None else None)
    app.state.ids = BookingIdGenerator(app.state.lease.node_id)
    app.mount("/metrics", make_asgi_app())

    @app.middleware("http")
    async def guard_and_measure(request: Request, call_next):
        t0 = time.perf_counter()
        if int(request.headers.get("content-length") or 0) > MAX_BODY:
            return JSONResponse({"detail": "request too large"}, status_code=413)
        response = await call_next(request)
        route = request.scope.get("route")
        path = getattr(route, "path", request.url.path)
        if not path.startswith("/metrics"):
            API_REQUESTS.labels(path, str(response.status_code)).inc()
            API_LATENCY.labels(path).observe(time.perf_counter() - t0)
        # security headers (SecOps) + which copy answered (shows load balancing)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["X-Replica"] = REPLICA
        if path != "/live":
            response.headers["Cache-Control"] = "no-store"
        return response

    def check_rate(request: Request, client_id: str | None):
        client = client_id or (request.client.host if request.client else "unknown")
        if not app.state.limiter.allow(client):
            log.warning("rate limited", client=client)
            raise HTTPException(429, "Too many requests - slow down and retry")

    def send(topic: str, key: str, ev: Envelope):
        if not app.state.sender.send(topic, key, ev.to_bytes()):
            raise HTTPException(503, "Queue unavailable - retry (same Idempotency-Key is safe)")

    # ---------------------------------------------------------------- rider
    @app.post("/v1/bookings", status_code=202, dependencies=[Depends(rider)], tags=["rider"])
    def create_booking(body: BookingRequest, request: Request,
                       idempotency_key: str = Header(..., min_length=4, max_length=100),
                       x_client_id: str | None = Header(None)):
        check_rate(request, x_client_id or f"user-{body.user_id}")
        state, cached = app.state.idem.begin(idempotency_key)
        if state == "DONE":
            return JSONResponse(cached, status_code=200, headers={"Idempotent-Replay": "true"})
        if state == "BUSY":
            raise HTTPException(409, "same Idempotency-Key is being processed - retry in a moment")

        pickup, dest = body.pickup_location, body.destination_location
        surge = app.state.surge.quote(pickup) if pickup in AREAS else 1.0
        fare = round(base_fare(pickup, dest) * surge, 2) if pickup in AREAS and dest in AREAS else 100.0
        booking_id = app.state.ids.next()
        now = csv_time(datetime.now(IST))
        row = {"booking_id": str(booking_id), "user_id": str(body.user_id), "driver_id": "",
               "pickup_location": pickup, "destination_location": dest, "booking_status": "REQUESTED",
               "fare": f"{fare:.2f}", "idempotency_key": idempotency_key, "created_at": now, "updated_at": now,
               "surge_multiplier": f"{surge:.1f}"}
        ev = booking_raw_event(row, source="booking-api")
        try:
            send(BOOKING_RAW, ev.kafka_key(), ev)
        except HTTPException:
            app.state.idem.abort(idempotency_key)
            log.error("kafka did not confirm, booking NOT accepted", booking_id=booking_id)
            raise
        if pickup in AREAS:
            app.state.surge.record_demand(pickup)
            SURGE.labels(pickup).set(surge)
        resp = {"booking_id": booking_id, "status": "ACCEPTED", "fare": f"{fare:.2f}",
                "surge_multiplier": surge, "idempotency_key": idempotency_key,
                "track": f"/v1/bookings/{booking_id}/stream"}
        app.state.idem.complete(idempotency_key, resp)
        log.info("booking accepted", booking_id=booking_id, key=idempotency_key, surge=surge, replica=REPLICA)
        return resp

    def read(booking_id: int):
        try:
            return app.state.reader(booking_id)
        except Exception as e:
            log.warning("status read failed", error=str(e)[:120])
            raise HTTPException(503, "status temporarily unavailable - your booking is safe in the queue") from e

    @app.get("/v1/bookings/{booking_id}", dependencies=[Depends(rider)], tags=["rider"])
    def get_booking(booking_id: int):
        ride = read(booking_id)
        if ride is None:
            raise HTTPException(404, "not stored yet - accepted bookings appear within seconds")
        return ride

    @app.get("/v1/bookings/{booking_id}/stream", tags=["rider"])
    async def stream(booking_id: int, key: str = Query(...)):
        if not _key_ok(key, config.RIDER_API_KEY, config.ADMIN_API_KEY):
            raise HTTPException(401, "wrong key")

        async def events():
            last, last_beat = None, time.time()
            for _ in range(900):                                   # at most 15 minutes
                try:
                    ride = await asyncio.to_thread(app.state.reader, booking_id)
                except Exception:
                    ride = {"status": "UNKNOWN", "note": "database busy, retrying"}
                if ride is not None and ride != last:
                    last = ride
                    yield f"data: {json.dumps(ride, default=str)}\n\n"
                    if ride.get("status") and is_final(ride):
                        return
                elif time.time() - last_beat > 15:
                    last_beat = time.time()
                    yield ": keep-alive\n\n"
                await asyncio.sleep(1)
        return StreamingResponse(events(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    @app.post("/v1/bookings/{booking_id}/cancel", status_code=202, dependencies=[Depends(rider)], tags=["rider"])
    def cancel(booking_id: int, request: Request):
        check_rate(request, f"cancel-{booking_id}")
        ev = Envelope(event_type="TripUpdate", booking_id=str(booking_id), source="booking-api",
                      payload={"booking_id": booking_id, "action": "CANCEL"})
        send(TRIP_UPDATES, str(booking_id), ev)
        return {"booking_id": booking_id, "status": "CANCEL_REQUESTED"}

    # ---------------------------------------------------------------- driver
    @app.post("/v1/drivers/{driver_id}/respond", status_code=202, dependencies=[Depends(driver)], tags=["driver"])
    def respond(driver_id: int, body: DriverAnswer, request: Request):
        check_rate(request, f"driver-{driver_id}")
        ev = Envelope(event_type="DriverResponse", booking_id=str(body.booking_id), source="booking-api",
                      payload={"booking_id": body.booking_id, "driver_id": driver_id, "action": body.action,
                               "responded_at": datetime.now(IST).isoformat()})
        send(DRIVER_RESPONSE, str(body.booking_id), ev)        # key = booking_id: one partition, in order
        log.info("driver response queued", booking_id=body.booking_id, driver_id=driver_id, action=body.action)
        return {"booking_id": body.booking_id, "driver_id": driver_id, "action": body.action,
                "status": "RESPONSE_QUEUED"}

    def trip(driver_id: int, booking_id: int, action: str, request: Request):
        check_rate(request, f"driver-{driver_id}")
        ev = Envelope(event_type="TripUpdate", booking_id=str(booking_id), source="booking-api",
                      payload={"booking_id": booking_id, "driver_id": driver_id, "action": action})
        send(TRIP_UPDATES, str(booking_id), ev)
        return {"booking_id": booking_id, "driver_id": driver_id, "status": f"{action}_REQUESTED"}

    @app.post("/v1/drivers/{driver_id}/trips/{booking_id}/start", status_code=202,
              dependencies=[Depends(driver)], tags=["driver"])
    def start(driver_id: int, booking_id: int, request: Request):
        return trip(driver_id, booking_id, "START", request)

    @app.post("/v1/drivers/{driver_id}/trips/{booking_id}/complete", status_code=202,
              dependencies=[Depends(driver)], tags=["driver"])
    def complete(driver_id: int, booking_id: int, request: Request):
        return trip(driver_id, booking_id, "COMPLETE", request)

    # ---------------------------------------------------------------- public
    @app.get("/v1/surge", tags=["public"])
    def surge_board():
        return app.state.surge.board()

    @app.get("/health", tags=["public"])
    def health():
        return {"status": "ok", "replica": REPLICA}

    @app.get("/ready", tags=["public"])
    def ready():
        redis_ok = False
        if app.state.redis is not None:
            try:
                redis_ok = bool(app.state.redis.ping())
            except Exception:
                redis_ok = False
        return {"status": "ready", "replica": REPLICA, "redis": redis_ok}

    @app.get("/live", response_class=HTMLResponse, tags=["public"])
    def live():
        return HTMLResponse((Path(__file__).parent / "live.html").read_text(encoding="utf-8"),
                            headers={"Content-Security-Policy":
                                     "default-src 'self'; script-src 'self' 'unsafe-inline'; "
                                     "style-src 'self' 'unsafe-inline'"})

    return app


app = create_app()
