-- Reference data so every user_id / driver_id / location in bookings.csv exists.
-- Owner: Person 2 (Database). CSV has user_id 7..1200 and driver_id 2..500.

INSERT INTO users (user_id, name, phone)
SELECT g, 'User ' || g, '+9190000' || lpad(g::text, 5, '0')
FROM generate_series(1, 1200) AS g
ON CONFLICT DO NOTHING;

INSERT INTO drivers (driver_id, name, vehicle_no)
SELECT g, 'Driver ' || g, 'KA-01-' || lpad(g::text, 4, '0')
FROM generate_series(1, 500) AS g
ON CONFLICT DO NOTHING;

INSERT INTO locations (name) VALUES
    ('Airport'), ('BTM Layout'), ('Banashankari'), ('Bellandur'), ('Electronic City'),
    ('HSR Layout'), ('Hebbal'), ('Indiranagar'), ('JP Nagar'), ('Jayanagar'),
    ('KR Puram'), ('Koramangala'), ('MG Road'), ('Majestic'), ('Marathahalli'),
    ('Rajajinagar'), ('Whitefield'), ('Yelahanka')
ON CONFLICT DO NOTHING;
