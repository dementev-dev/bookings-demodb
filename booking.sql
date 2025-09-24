/*
Demonstration Database Generator

Copyright (c) 2025 Postgres Professional

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

--
-- Routines related to the booking process
--

/*
  Get a path from one airport to another.
*/
CREATE OR REPLACE FUNCTION get_path(
    airport_from char(3),
    airport_to char(3),
    at timestamptz,
    hops OUT text[],
    flights OUT integer[],
    flight_time OUT interval,
    last_arrival_time OUT timestamptz
)
AS $$
    WITH RECURSIVE p(
        last_arrival,
        last_arrival_time,
        destination,
        hops,
        flights,
        flight_time,
        found
    ) AS (
        SELECT a_from.airport_code,
          NULL::timestamptz,
          a_to.airport_code,
          array[a_from.airport_code],
          array[]::integer[],
          interval '0 sec',
          a_from.airport_code = a_to.airport_code
        FROM bookings.airports_data a_from,
          bookings.airports_data a_to
        WHERE a_from.airport_code = get_path.airport_from
          AND a_to.airport_code = get_path.airport_to
        UNION ALL
        SELECT f.arrival_airport,
          f.scheduled_arrival,
          p.destination,
          (p.hops || f.arrival_airport)::char(3)[],
          p.flights || f.flight_id,
          p.flight_time + f.duration,
          bool_or(f.arrival_airport = p.destination) OVER ()
        FROM p
          CROSS JOIN LATERAL (
              -- nearest flights
              SELECT arrival_airport,
                min(scheduled_arrival) AS scheduled_arrival,
                min(flight_id) AS flight_id,
                min(duration) AS duration
              FROM (
                  SELECT r.arrival_airport,
                    first_value(f.scheduled_arrival) OVER (w) AS scheduled_arrival,
                    first_value(f.flight_id) OVER (w) AS flight_id,
                    first_value(r.duration) OVER (w) AS duration
                  FROM bookings.routes r
                    JOIN bookings.flights f ON r.route_no = f.route_no
                                          AND r.validity @> f.scheduled_departure
                    JOIN gen.seats_remain sr ON sr.flight_id = f.flight_id
                  WHERE r.departure_airport = p.last_arrival
                    AND lower(r.validity) > get_path.at + OPEN_REGISTRATION_INTERVAL() + BOOKINGS_SAFEGUARD_INTERVAL() - ROUTES_DURATION()
                    -- ensure that flight is scheduled and we have some time before rigistration is open
                    -- (checking status is not egough, because status can change to 'In time' in parallel
                    -- process wilte we are busy making our booking, and we'll miss registration)
                    AND f.status = 'Scheduled'
                    AND f.scheduled_departure > get_path.at + OPEN_REGISTRATION_INTERVAL() + BOOKINGS_SAFEGUARD_INTERVAL()
                    -- don't consider fully booked flights
                    AND sr.available > 0 -- doesn't guarantee availability! no locking here
                    -- ensure minimal transfer time
                    AND f.scheduled_departure > coalesce(p.last_arrival_time, get_path.at) +
                        current_setting('gen.min_transfer')::float * interval '1 hour'
                    -- don't want to wait too long
                    AND (p.last_arrival_time IS NULL OR
                         f.scheduled_departure < p.last_arrival_time +
                         current_setting('gen.max_transfer')::float * interval '1 hour'
                        )
                    -- prevent loops
                    AND NOT r.arrival_airport = ANY(p.hops)
                    -- limit hops
                    AND cardinality(p.hops) <= current_setting('gen.max_hops')::integer
                  WINDOW w AS (PARTITION BY r.arrival_airport ORDER BY f.scheduled_departure)
              ) t
              GROUP BY arrival_airport
          ) f
        WHERE NOT p.found
    )
    SELECT hops, flights, flight_time, last_arrival_time
    FROM p
    WHERE p.last_arrival = p.destination
    ORDER BY flight_time
    LIMIT 1;
$$ LANGUAGE sql;

/*
  Get a random booking ref.
*/
-- We use [A-Z0-9]{6}. Capacity can be increased by adding lowercase letters.
CREATE OR REPLACE FUNCTION get_book_ref()
RETURNS char(6)
AS $$
DECLARE
  r integer;
  d integer;
  i integer;
  book_ref text;
BEGIN
  -- bookref capacity is (26+10)^6 = 2,176,782,336
  -- abs(integer) capacity is 2^31 = 2,147,483,648
  r := abs(hashfloat8(random()));
  book_ref := '';
  FOR i IN 1..6 LOOP
      d := r % 36;
      r := r / 36;
      book_ref := book_ref || CASE
          WHEN d < 10 THEN chr(ascii('0') + d)
          ELSE chr(ascii('A') + d - 10)
      END;
  END LOOP;
  RETURN book_ref;
END;
$$ LANGUAGE plpgsql;

/*
  Create a booking.
  Inserts a booking with a unique book_ref, returns the book_ref
  and the number of retries it took to generate unique number (for monitoring).
*/
CREATE OR REPLACE FUNCTION create_booking(
    at       IN  timestamptz,
    book_ref OUT char(6),
    retries  OUT integer
)
AS $$
BEGIN
    retries := 0;
    LOOP
      book_ref := get_book_ref();
      BEGIN
          INSERT INTO bookings.bookings(book_ref, book_date, total_amount)
              VALUES (book_ref, at, 0.0);
          RETURN;
      EXCEPTION
      WHEN unique_violation THEN
          retries := retries + 1;
      END;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

/*
  Get next ticket number.
*/
CREATE OR REPLACE FUNCTION get_ticket_no()
RETURNS text
AS $$
  SELECT to_char(nextval('gen.ticket_s'),'FM0000000000000');
$$ LANGUAGE sql;

/*
  Create a ticket.
*/
CREATE OR REPLACE FUNCTION create_ticket(
    book_ref char(6),
    passenger_id text,
    passenger_name text,
    outbound boolean
)
RETURNS text
AS $$
DECLARE
    l_ticket_no text;
BEGIN
    INSERT INTO bookings.tickets(ticket_no, book_ref, passenger_id, passenger_name, outbound)
    VALUES (get_ticket_no(), book_ref, passenger_id, passenger_name, outbound)
    RETURNING ticket_no INTO l_ticket_no;

    RETURN l_ticket_no;
END;
$$ LANGUAGE plpgsql;

/*
  Get an available fare condition, if any.
*/
CREATE OR REPLACE FUNCTION get_fare_conditions(flight_id integer)
RETURNS text
AS $$
WITH avail AS (
    SELECT s.fare_conditions
    FROM bookings.flights f
      JOIN bookings.routes r ON r.validity @> f.scheduled_departure
                           AND r.route_no = f.route_no
      JOIN bookings.seats s ON s.airplane_code = r.airplane_code
    WHERE f.flight_id = get_fare_conditions.flight_id
    EXCEPT ALL
    SELECT s.fare_conditions
    FROM bookings.segments s
    WHERE s.flight_id = get_fare_conditions.flight_id
)
SELECT fare_conditions
FROM avail
ORDER BY random()
LIMIT 1;
$$ LANGUAGE sql;

/*
  Get a random available seat number.
*/
CREATE OR REPLACE FUNCTION get_seat_no(flight_id integer, fare_conditions text)
RETURNS text
AS $$
WITH cabin AS (
    SELECT s.seat_no
    FROM bookings.flights f
      JOIN bookings.routes r ON r.validity @> f.scheduled_departure
                            AND r.route_no = f.route_no
      JOIN bookings.seats s ON s.airplane_code = r.airplane_code
    WHERE f.flight_id = get_seat_no.flight_id
      AND s.fare_conditions = get_seat_no.fare_conditions
), booked AS (
    SELECT b.seat_no
    FROM bookings.boarding_passes b
    WHERE b.flight_id = get_seat_no.flight_id
)
SELECT seat_no
FROM (
    SELECT seat_no FROM cabin
    EXCEPT ALL
    SELECT seat_no FROM booked
) t
ORDER BY random()
LIMIT 1;
$$ LANGUAGE sql;

/*
  Get ticket price.
*/
CREATE OR REPLACE FUNCTION get_price(flight_id integer, fare_conditions text)
RETURNS numeric
AS $$
    SELECT round( extract(epoch FROM r.duration)::numeric/60 * -- minutes of flight
                  CASE fare_conditions
                    WHEN 'Economy' THEN 1.0
                    WHEN 'Comfort' THEN 1.3
                    WHEN 'Business' THEN 2.0
                  END *
                  current_setting('gen.exchange')::numeric,
                  0 -- do we need rounding?..
                )
    FROM bookings.flights f
      JOIN bookings.routes r ON r.route_no = f.route_no
                           AND r.validity @> f.scheduled_departure
    WHERE f.flight_id = get_price.flight_id;
$$ LANGUAGE sql;

/*
  Create a segment.

  Throws an exception if:
  - no seat is available
  - the same passenger ID appears twice on the same flight
*/
CREATE OR REPLACE PROCEDURE create_segment(ticket_no text, flight_id integer)
AS $$
DECLARE
    l_fare_conditions text;
    l_price numeric;
BEGIN
    -- create segment
    l_fare_conditions := get_fare_conditions(flight_id);

    IF l_fare_conditions IS NULL THEN
        RAISE SQLSTATE 'NSEAT';
    END IF;

    l_price := get_price(flight_id, l_fare_conditions);

    INSERT INTO bookings.segments(ticket_no, flight_id, fare_conditions, price)
    VALUES (ticket_no, flight_id, l_fare_conditions, l_price);

    -- update total_amount
    UPDATE bookings.bookings
    SET total_amount = total_amount + l_price
    WHERE book_ref = (
        SELECT t.book_ref FROM bookings.tickets t WHERE t.ticket_no = create_segment.ticket_no
    );

    -- decrement available seats
    UPDATE gen.seats_remain sr
    SET available = sr.available - 1
    WHERE sr.flight_id = create_segment.flight_id;
END;
$$ LANGUAGE plpgsql;

/*
  Make a booking.
*/
CREATE OR REPLACE PROCEDURE make_booking(evt gen.events)
AS $$
DECLARE
    airport_from char(3);
    airport_to char(3);

    outb_flights integer[];
    inb_flights integer[];

    outb_ticket_no text;
    inb_ticket_no text;

    outb_last_arrival_time timestamptz;
    inb_last_arrival_time timestamptz;

    npass integer;
    country_code text;

    l_book_ref char(6);
    l_retries integer;
    l_flight_id integer;
    l_passenger_id text;
    l_passenger_name text;
    back timestamptz;

    rate float;
    next timestamptz;
BEGIN
    airport_from := evt.payload->>'airport';
    airport_to := airport_to(airport_from);

    CALL log_message(1, format('  %s -> %s', airport_from, airport_to));

    -- try to make a booking
    BEGIN
        npass := rnd_binomial(
            current_setting('gen.max_pass_per_booking')::integer - 1,
            0.1
        ) + 1;
        country_code := a.country_code FROM gen.airports_data a WHERE a.airport_code = airport_from;

        -- search for a hops-optimized path, starting at nearest possible time
        SELECT flights, last_arrival_time
        INTO outb_flights, outb_last_arrival_time
        FROM get_path(airport_from, airport_to, evt.at);

        -- give up if there is no path
        IF outb_flights IS NULL THEN
            RAISE SQLSTATE 'NPATH';
        END IF;

        -- roundtrip ticket?
        IF random() < current_setting('gen.roundtrip_frac')::float THEN

            -- search for a roundtrip path after some random delay
            back := outb_last_arrival_time + rnd_binomial(30,0.25) * interval '1' day;
            SELECT flights, last_arrival_time
            INTO inb_flights, inb_last_arrival_time
            FROM get_path(airport_to, airport_from, back);

            -- inb_flights can be NULL, meaning there is no roundtrip path;
            -- it's okay, we'll book one direction
        END IF;

        -- lock flights in advance in a fixed order to prevent deadlocks
        PERFORM pg_advisory_xact_lock(fid)
        FROM unnest(outb_flights || inb_flights) fid -- coalesce is not needed
        ORDER BY fid;

        -- create a booking
        SELECT *
        FROM create_booking(evt.at)
        INTO l_book_ref, l_retries;

        -- create tickets
        FOR i IN 1 .. npass
        LOOP
            SELECT passenger_id, passenger_name
            FROM get_passenger(country_code, evt.at, coalesce(inb_last_arrival_time, outb_last_arrival_time))
            INTO l_passenger_id, l_passenger_name;

            -- outbound
            outb_ticket_no := create_ticket(l_book_ref, l_passenger_id, l_passenger_name, true);
            FOR l_flight_id IN (
                SELECT fid
                FROM unnest(outb_flights) fid
                ORDER BY fid
            )
            LOOP
                CALL create_segment(outb_ticket_no, l_flight_id);
            END LOOP;

            -- inbound, if any
            IF inb_flights IS NOT NULL THEN
                inb_ticket_no := create_ticket(l_book_ref, l_passenger_id, l_passenger_name, false);
                FOR l_flight_id IN (
                    SELECT fid
                    FROM unnest(inb_flights) fid
                    ORDER BY fid
                )
                LOOP
                    CALL create_segment(inb_ticket_no, l_flight_id);
                END LOOP;
            END IF;
        END LOOP;

        UPDATE gen.stat_bookings
        SET success = success + 1,
            forceoneway = forceoneway + (inb_flights IS NULL)::integer;
    EXCEPTION
        -- already created booking/tickets/segments are rolled back
        WHEN SQLSTATE 'NPATH' THEN
            UPDATE gen.stat_bookings SET nopath = nopath + 1;
        WHEN SQLSTATE 'NSEAT' THEN
            UPDATE gen.stat_bookings SET noseat = noseat + 1;
    END;

    IF l_retries IS NOT NULL THEN
        UPDATE gen.stat_bookrefs SET retries = retries + l_retries;
    END IF;

    rate := week_bookings(traffic::float) FROM gen.airports_data WHERE airport_code = airport_from;
    next := evt.at + rnd_exponential(rate) * interval '7 days';
    INSERT INTO gen.events(at,type,payload)
        VALUES (next, 'BOOKING', jsonb_object(ARRAY['airport',airport_from]));
END;
$$ LANGUAGE plpgsql;

/*
  Missed this or some previous flight?
  Returns true if so, and false otherwise.
  Note that actual departure of flight_id is not yet in the table, so we pass it explicitly.
*/
CREATE OR REPLACE FUNCTION miss_flight(actual_departure timestamptz, ticket_no text, flight_id integer)
RETURNS boolean
AS $$
DECLARE
    missed boolean;
BEGIN
    -- first see if this ticket has arleary missed some previous flight
    SELECT true
    INTO missed
    FROM gen.missed_flights m
    WHERE m.ticket_no = miss_flight.ticket_no;

    IF missed THEN
        RETURN missed;
    END IF;

    -- check if the ticket has just missed the flight in question
    -- (actual_arrival of the previous flight may not be known yet if that flight has not yet arrived)
    SELECT coalesce(f_prev.actual_arrival,miss_flight.actual_departure) > miss_flight.actual_departure - MISS_FLIGHT_INTERVAL()
    INTO missed
    FROM bookings.segments s
      JOIN bookings.flights f_prev ON f_prev.flight_id = s.flight_id
      JOIN bookings.flights f_curr ON f_curr.flight_id = miss_flight.flight_id
    WHERE s.ticket_no = miss_flight.ticket_no
      AND f_prev.scheduled_departure < f_curr.scheduled_departure
    ORDER BY f_prev.scheduled_departure DESC
    LIMIT 1;

    -- if so, remember it
    IF missed THEN
        INSERT INTO gen.missed_flights(ticket_no)
        VALUES (miss_flight.ticket_no);
    END IF;

    RETURN coalesce(missed,false);
END;
$$ LANGUAGE plpgsql;

