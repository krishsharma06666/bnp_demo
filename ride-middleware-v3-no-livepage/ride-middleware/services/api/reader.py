"""Read-only view of a ride for the rider app and the live tracker. Owner: Person 3."""

RIDE = """
SELECT b.booking_id, b.booking_status, b.pickup_location, b.destination_location, b.fare, b.surge_multiplier,
       b.dispatch_round, b.driver_id, d.driver_name, v.registration_number, v.vehicle_type, v.model,
       (SELECT count(*) FROM driver_responses r WHERE r.booking_id = b.booking_id)                 AS offers,
       (SELECT count(*) FROM driver_responses r WHERE r.booking_id = b.booking_id
                                                  AND r.response = 'REJECTED')                     AS rejected,
       (SELECT p.payment_status FROM payments p WHERE p.booking_id = b.booking_id LIMIT 1)         AS payment
  FROM bookings b
  LEFT JOIN drivers d  ON d.driver_id = b.driver_id
  LEFT JOIN vehicles v ON v.driver_id = b.driver_id
 WHERE b.booking_id = %s
"""
TERMINAL = {"CANCELLED", "NO_DRIVER", "FAILED"}


def get_ride(conn, booking_id: int) -> dict | None:
    row = conn.execute(RIDE, (booking_id,)).fetchone()
    if row is None:
        return None
    (bid, status, pickup, dest, fare, surge, rnd, driver_id, driver_name, reg, vtype, model,
     offers, rejected, payment) = row
    ride = {"booking_id": bid, "status": status, "pickup": pickup, "destination": dest,
            "fare": str(fare), "surge_multiplier": str(surge), "dispatch_round": rnd,
            "offers_sent": offers, "offers_rejected": rejected, "payment": payment,
            "driver": None}
    if driver_id:
        ride["driver"] = {"driver_id": driver_id, "name": driver_name, "vehicle": f"{vtype} {model}".strip(),
                          "registration": reg}
    return ride


def is_final(ride: dict) -> bool:
    return ride["status"] in TERMINAL or (ride["status"] == "COMPLETED" and ride["payment"] is not None)
