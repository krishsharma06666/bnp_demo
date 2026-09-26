-- 003: reference data (Person 2). Safe to re-run: ON CONFLICT DO NOTHING.
-- If your team already loaded its own users/drivers (e.g. from the "driver Info" sheet), those rows are kept.

INSERT INTO locations (name, lat, lng) VALUES
  ('Airport', 13.1986, 77.7066), ('BTM Layout', 12.9166, 77.6101), ('Banashankari', 12.9255, 77.5468),
  ('Bellandur', 12.9304, 77.6784), ('Electronic City', 12.8452, 77.6602), ('HSR Layout', 12.9116, 77.6474),
  ('Hebbal', 13.0354, 77.5970), ('Indiranagar', 12.9719, 77.6412), ('JP Nagar', 12.9063, 77.5857),
  ('Jayanagar', 12.9308, 77.5838), ('KR Puram', 13.0076, 77.6958), ('Koramangala', 12.9352, 77.6245),
  ('MG Road', 12.9756, 77.6050), ('Majestic', 12.9767, 77.5713), ('Marathahalli', 12.9591, 77.6974),
  ('Rajajinagar', 12.9912, 77.5561), ('Whitefield', 12.9698, 77.7500), ('Yelahanka', 13.1007, 77.5963)
ON CONFLICT DO NOTHING;

-- 1,200 riders (bookings.csv uses user_id 7..1200)
INSERT INTO users (user_id, full_name, email, phone, status, created_at)
SELECT g, 'Rider ' || g, 'rider' || g || '@example.com', 919000000000 + g, 'ACTIVE', now()
FROM generate_series(1, 1200) AS g
ON CONFLICT DO NOTHING;

-- 500 drivers spread over the 18 areas (bookings.csv uses driver_id 2..500)
INSERT INTO drivers (driver_id, driver_name, phone, availability_status, version_no, created_at, current_location)
SELECT g, 'Driver ' || g, 918000000000 + g, 'AVAILABLE', 1, now(),
       (ARRAY['Airport','BTM Layout','Banashankari','Bellandur','Electronic City','HSR Layout','Hebbal',
              'Indiranagar','JP Nagar','Jayanagar','KR Puram','Koramangala','MG Road','Majestic',
              'Marathahalli','Rajajinagar','Whitefield','Yelahanka'])[1 + g % 18]
FROM generate_series(1, 500) AS g
ON CONFLICT DO NOTHING;

-- drivers that existed before this migration get a location too
UPDATE drivers SET current_location =
       (ARRAY['Airport','BTM Layout','Banashankari','Bellandur','Electronic City','HSR Layout','Hebbal',
              'Indiranagar','JP Nagar','Jayanagar','KR Puram','Koramangala','MG Road','Majestic',
              'Marathahalli','Rajajinagar','Whitefield','Yelahanka'])[1 + (driver_id % 18)::int]
 WHERE current_location IS NULL;

-- one vehicle per driver
INSERT INTO vehicles (vehicle_id, driver_id, registration_number, vehicle_type, manufacturer, model, status, created_at)
SELECT g, g, 'KA-01-' || lpad(g::text, 4, '0'),
       (ARRAY['Mini','Sedan','SUV','Auto'])[1 + g % 4],
       (ARRAY['Maruti','Hyundai','Toyota','Bajaj'])[1 + g % 4],
       (ARRAY['Swift','Aura','Innova','RE'])[1 + g % 4],
       'ACTIVE', now()
FROM generate_series(1, 500) AS g
ON CONFLICT DO NOTHING;
