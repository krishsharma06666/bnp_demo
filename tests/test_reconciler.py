"""Reconciler (Person 6): COMPLETED bookings without a payment are found and re-sent."""
from services.reconciler.reconciler import find_missing

ROW = """INSERT INTO bookings (booking_id,user_id,driver_id,pickup_location,destination_location,
         booking_status,fare,idempotency_key,created_at,updated_at)
         VALUES (%s,57,279,'Indiranagar','BTM Layout',%s,115.91,%s,now(),now())"""


def test_missing_payments_found_and_grace_respected(db):
    for b, st in [(1, "COMPLETED"), (2, "COMPLETED"), (3, "STARTED"), (4, "COMPLETED")]:
        db.execute(ROW, (b, st, f"K{b}"))
    db.execute("""INSERT INTO payments (payment_id, booking_id, user_id, amount, payment_method, payment_status,
                  idempotency_key, transaction_reference, created_at) VALUES (1,1,57,115.91,'UPI','COMPLETED','PAY-1','T1',now())""")
    db.commit()
    count, resend = find_missing(db, grace_seconds=0)
    assert count == 2 and sorted(b for b, _, _ in resend) == [2, 4]        # 3 is not COMPLETED
    assert all(u == 57 for _, _, u in resend)
    count, resend = find_missing(db, grace_seconds=3600)                   # too fresh: count, don't resend
    assert count == 2 and resend == []
