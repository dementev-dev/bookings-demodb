/*
Demonstration Database Generator

Copyright (c) 2025 Postgres Professional

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

--
-- Generation engine
--

-- Constants

/*
  Time to open flights for bookings.
*/
CREATE OR REPLACE FUNCTION OPEN_BOOKINGS_INTERVAL()
RETURNS interval IMMUTABLE LANGUAGE sql
RETURN interval '60 days';

/*
  Gap between last booking for a flight and start of registration.
  Without such safeguard, CHECK-IN event can start earlier than BOOKING completes.
*/
CREATE OR REPLACE FUNCTION BOOKINGS_SAFEGUARD_INTERVAL()
RETURNS interval IMMUTABLE LANGUAGE sql
RETURN interval '15 min';

/*
  Time to open registration for a flight.
*/
CREATE OR REPLACE FUNCTION OPEN_REGISTRATION_INTERVAL()
RETURNS interval IMMUTABLE LANGUAGE sql
RETURN interval '1 day';

/*
  Time to close registration for a flight.
*/
CREATE OR REPLACE FUNCTION CLOSE_REGISTRATION_INTERVAL()
RETURNS interval IMMUTABLE LANGUAGE sql
RETURN interval '40 mins'; -- must be greater that OPEN_BOARDING_INTERVAL

/*
  Time to board.
*/
CREATE OR REPLACE FUNCTION OPEN_BOARDING_INTERVAL()
RETURNS interval IMMUTABLE LANGUAGE sql
RETURN interval '30 mins';

/*
  Time to close boarding.
*/
CREATE OR REPLACE FUNCTION CLOSE_BOARDING_INTERVAL()
RETURNS interval IMMUTABLE LANGUAGE sql
RETURN interval '10 mins'; -- must be less than OPEN_BOARDING_INTERVAL

/*
  Miss-the-flight interval between actual arrival of the previous flight
  and actual departure of the flight in question.
*/
CREATE OR REPLACE FUNCTION MISS_FLIGHT_INTERVAL()
RETURNS interval IMMUTABLE LANGUAGE sql
RETURN interval '40 mins';

/*
  Time for new routes to take effect.
*/
CREATE OR REPLACE FUNCTION ROUTES_TAKE_EFFECT()
RETURNS interval IMMUTABLE LANGUAGE sql
RETURN interval '2 month';

/*
  Interval of re-generatiion of routes.
*/
CREATE OR REPLACE FUNCTION ROUTES_DURATION()
RETURNS interval IMMUTABLE LANGUAGE sql
RETURN interval '1 month';

-- Functions

/*
  Logging.
*/
CREATE OR REPLACE PROCEDURE log_message(severity integer, message text)
AS $$
BEGIN
    IF severity <= current_setting('gen.log_severity')::integer THEN
        INSERT INTO gen.log(severity,message) VALUES (severity,message);
    END IF;
END;
$$
LANGUAGE plpgsql;

/*
  Generate database within the given time frame using specified number of jobs.
  Wait till the end of the process or terminate with CALL abort().
*/
CREATE OR REPLACE PROCEDURE generate(
    start_date timestamptz,
    end_date timestamptz,
    jobs integer DEFAULT 1
)
AS $$
BEGIN
    IF busy() THEN
        RAISE EXCEPTION 'Generation is already in progress. Wait until it ends or abort.';
    END IF;

    PERFORM setseed(0.0);
    TRUNCATE TABLE gen.events;
    TRUNCATE TABLE gen.events_history;
    -- INIT (and hence the subsequent BUILD ROUTE) must be placed so that the first bookings
    -- will start at start_date
    INSERT INTO gen.events(at,type) VALUES (start_date + ROUTES_DURATION() - ROUTES_TAKE_EFFECT(), 'INIT');

    -- zero stats counters
    TRUNCATE TABLE gen.stat_bookings;
    INSERT INTO gen.stat_bookings(success,forceoneway,nopath,noseat)
        VALUES (0,0,0,0);
    TRUNCATE TABLE gen.stat_bookrefs;
    INSERT INTO gen.stat_bookrefs(retries) VALUES (0);

    -- empty log
    TRUNCATE TABLE gen.log;

    --- empty utility tables
    TRUNCATE TABLE gen.seats_remain;
    TRUNCATE TABLE gen.missed_flights;

    COMMIT;

    CALL continue(end_date, jobs);
END;
$$
LANGUAGE plpgsql;

/*
  Continue generation after generate() stops.
*/
CREATE OR REPLACE PROCEDURE continue(
    end_date timestamptz,
    jobs integer DEFAULT 1
)
AS $$
DECLARE
    connname text;
    res text;
    start_date timestamptz;
    duration interval;
BEGIN
    IF busy() THEN
        RAISE EXCEPTION 'Generation is already in progress. Wait until it ends or abort.';
    END IF;
    IF jobs < 1 THEN
        RAISE EXCEPTION 'Parameter jobs must be greater or equal to 1';
    END IF;

    TRUNCATE TABLE gen.stat_jobs;

    -- disconnect all previously opened connections
    PERFORM dblink_disconnect(unnest(dblink_get_connections()));

    -- start parallel jobs
    FOR i IN 1 .. jobs LOOP
        connname := 'job' || i;
        PERFORM dblink_connect(connname, current_setting('gen.connstr'));
        res := CASE dblink_send_query(connname, format('CALL process_queue(%L)',end_date))
            WHEN 1 THEN 'ok' ELSE 'FAILED'
        END CASE;
        CALL log_message(0, format('Job %s (connname=%s): %s', i, connname, res));
        RAISE NOTICE 'Starting job %: %', i, res;
    END LOOP;

    -- re-create booking.now()
    EXECUTE format(
        'CREATE OR REPLACE FUNCTION bookings.now() RETURNS timestamptz
         LANGUAGE sql IMMUTABLE
         RETURN %L::timestamptz;',
        end_date);

    -- re-create booking.version()
    start_date := (
        SELECT at FROM gen.events WHERE type = 'INIT'
        UNION ALL
        SELECT at FROM gen.events_history WHERE type = 'INIT'
    ) - ROUTES_DURATION() + ROUTES_TAKE_EFFECT();
    duration := end_date - start_date;
    EXECUTE format(
        'CREATE OR REPLACE FUNCTION bookings.version() RETURNS text
         LANGUAGE sql IMMUTABLE
         RETURN ''%s %s (%s)'';',
        current_setting('gen.airlines_name'), start_date::date, date_trunc('day',duration));
END;
$$
LANGUAGE plpgsql;

/*
  Aborts generation.
  You should not continue() after this, only generate() anew.
*/
CREATE OR REPLACE PROCEDURE abort()
AS $$
BEGIN
    -- disconnect all previously opened connections
    PERFORM dblink_disconnect(unnest(dblink_get_connections()));

    -- terminate all processors
    PERFORM pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'Airlines processor';

    -- re-create booking.now() returning NULL
    EXECUTE
        'CREATE OR REPLACE FUNCTION bookings.now() RETURNS timestamptz
         LANGUAGE sql IMMUTABLE
         RETURN NULL::timestamptz;';
END;
$$
LANGUAGE plpgsql;

/*
  Is generation in progress?
*/
CREATE OR REPLACE FUNCTION busy() RETURNS boolean
LANGUAGE sql
BEGIN ATOMIC
    SELECT count(*) > 0
    FROM pg_stat_activity
    WHERE application_name = 'Airlines processor'
      AND state != 'idle';
END;

/*
  Takes the next event and processes it.
  Several instances of this procedure can be run in parallel.
*/
CREATE OR REPLACE PROCEDURE process_queue(end_date timestamptz)
AS $$
DECLARE
    evt gen.events;
BEGIN
    SET application_name = 'Airlines processor';

    LOOP
        -- get next event and mark it as being processed (pid)

        SELECT *
        INTO evt
        FROM gen.events
        WHERE pid IS NULL
        ORDER BY at LIMIT 1
        FOR UPDATE SKIP LOCKED;

        IF evt.at >= end_date THEN
            CALL log_message(0, 'End date reached, exiting');
            EXIT;
        END IF;
        IF evt.event_id IS NULL THEN
            PERFORM pg_sleep(1);
            CONTINUE;
        END IF;

        UPDATE gen.events
        SET pid = pg_backend_pid()
        WHERE event_id = evt.event_id;

        COMMIT;

        -- process event

        DECLARE
            detail text;
            ctx text;
        BEGIN
            CALL process_event(evt, end_date);
        EXCEPTION
            WHEN others THEN
                GET STACKED DIAGNOSTICS
                    detail := PG_EXCEPTION_DETAIL,
                    ctx := PG_EXCEPTION_CONTEXT;
                CALL log_message(0, 'Error at event '||evt.event_id||': '||SQLERRM||E'\n'||detail);
                CALL log_message(0, E'Call stack: \n'||ctx);
                COMMIT;
                EXIT;
        END;

        -- finalize processing

        WITH d AS (
          DELETE FROM gen.events
          WHERE event_id = evt.event_id
          RETURNING *
        )
        INSERT INTO gen.events_history SELECT * FROM d;

        INSERT INTO gen.stat_jobs(pid, events) VALUES (pg_backend_pid(), 0)
        ON CONFLICT ON CONSTRAINT stat_jobs_pkey DO UPDATE SET events = stat_jobs.events + 1;

        COMMIT;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

/*
  Process an event.
*/
CREATE OR REPLACE PROCEDURE process_event(evt gen.events, end_date timestamptz)
AS $$
BEGIN
    CASE evt.type
        WHEN 'INIT' THEN
            CALL do_init(evt);
        WHEN 'BUILD ROUTES' THEN -- build new routes
            CALL build_routes(evt);
        WHEN 'BOOKING' THEN -- make a booking
            CALL make_booking(evt);
        WHEN 'FLIGHT' THEN -- open bookings for the flight
            CALL open_booking(evt);
        WHEN 'REGISTRATION' THEN -- open registration for the flight
            CALL registration(evt);
        WHEN 'CHECK-IN' THEN -- check-in
            CALL check_in(evt);
        WHEN 'BOARDING' THEN -- open boarding for the flight
            CALL boarding(evt, end_date);
        WHEN 'GET IN' THEN -- board the aircraft
            CALL get_in(evt);
        WHEN 'TAKEOFF' THEN -- start the flight
            CALL takeoff(evt);
        WHEN 'LANDING' THEN -- end the flight
            CALL landing(evt);
        WHEN 'VACUUM' THEN -- vacuum queue
            CALL vacuum(evt);
        WHEN 'MONITORING' THEN -- monitoring
            CALL monitoring(evt);
    END CASE;
END;
$$ LANGUAGE plpgsql;

/*
  Initialization
*/
CREATE OR REPLACE PROCEDURE do_init(evt gen.events)
AS $$
DECLARE
    start_date timestamptz;
    airport char(3);
    rate float;
    next timestamptz;
BEGIN
    start_date := (evt.payload->>'start')::timestamptz;

    -- copy data from gen-tables
    TRUNCATE TABLE bookings.airports_data CASCADE;
    INSERT INTO bookings.airports_data(
      airport_code, airport_name, city, country, coordinates, timezone
    )
    SELECT airport_code, airport_name, city, country, coordinates, timezone
    FROM gen.airports_data;

    TRUNCATE TABLE bookings.airplanes_data CASCADE;
    INSERT INTO bookings.airplanes_data(
      airplane_code, model, range, speed
    )
    SELECT airplane_code, model, range, speed
    FROM gen.airplanes_data;

    TRUNCATE TABLE bookings.seats;
    INSERT INTO bookings.seats(
      airplane_code, seat_no, fare_conditions
    )
    SELECT airplane_code, seat_no, fare_conditions
    FROM gen.seats;

    TRUNCATE TABLE bookings.bookings CASCADE;
    TRUNCATE TABLE bookings.tickets CASCADE;
    TRUNCATE TABLE bookings.segments CASCADE;
    TRUNCATE TABLE bookings.routes CASCADE;
    TRUNCATE TABLE bookings.flights CASCADE;

    ALTER SEQUENCE gen.ticket_s RESTART WITH 5432000000;
    ALTER SEQUENCE bookings.flights_flight_id_seq RESTART;

    -- pre-calculate probabilities to choose an airport
    -- (it doesn't depend on routes, which can change over time)
    TRUNCATE TABLE gen.airport_to_prob;
    INSERT INTO gen.airport_to_prob(
        departure_airport,
        arrival_airport,
        domestic,
        cume_dist
    )
    WITH cts AS (
      -- number of cities per country
      SELECT country_code, count(DISTINCT city) cities
        FROM gen.airports_data
       WHERE traffic IS NOT NULL
       GROUP BY country_code
    )
    SELECT departure_airport,
           arrival_airport,
           domestic,
           sum(chance) OVER (PARTITION BY departure_airport, domestic ORDER BY arrival_airport) /
           sum(chance) OVER (PARTITION BY departure_airport, domestic)
           AS cume_dist
      FROM (
            SELECT a_from.airport_code AS departure_airport,
                   a_to.airport_code AS arrival_airport,
                   CASE WHEN cts.cities > 1 THEN a_from.country_code = a_to.country_code
                     ELSE NULL -- for countires with just one city
                   END AS domestic,
                   chance_to_fly(a_to.traffic, a_from.coordinates <@> a_to.coordinates) AS chance
              FROM gen.airports_data a_to
                   CROSS JOIN gen.airports_data a_from
                   JOIN cts ON a_from.country_code = cts.country_code
             WHERE a_from.traffic IS NOT NULL
               AND a_to.traffic IS NOT NULL
               AND a_from.city != a_to.city
           ) t;

    -- re-calculate cume_dist just in case
    CALL calc_names_cume_dist();

    -- generate schedule
    INSERT INTO gen.events(at,type,payload)
        VALUES (evt.at, 'BUILD ROUTES', jsonb_object(ARRAY['start',(evt.at + ROUTES_TAKE_EFFECT())::text,'period','0']));

    -- seed bookings
    FOR airport, rate IN (
        SELECT airport_code, week_bookings(traffic::float)
        FROM gen.airports_data
        WHERE traffic IS NOT NULL
    )
    LOOP
        -- bookings are modelled with the Poisson process
        next := evt.at + interval '1 month' + rnd_exponential(rate) * interval '7 days';
        INSERT INTO gen.events(at,type,payload)
            VALUES (next, 'BOOKING', jsonb_object(ARRAY['airport',airport]));
    END LOOP;

    -- seed vacuum
    next := evt.at + interval '1 month' + interval '2 days';
    INSERT INTO gen.events(at,type)
        VALUES (next, 'VACUUM');

    -- seed monitoring
    next := evt.at + interval '1 month' + interval '1 day';
    INSERT INTO gen.events(at,type,payload)
        VALUES (next, 'MONITORING', json_object(ARRAY[
            'now',now()::text,'bookings','0','bpasses','0',
            'b_success','0','b_forceoneway','0','b_nopath','0','b_noseat','0','b_retries','0']));
END;
$$ LANGUAGE plpgsql;

/*
  Open bookings for the flight.
*/
CREATE OR REPLACE PROCEDURE open_booking(evt gen.events)
AS $$
DECLARE
    next timestamptz;
    l_route_no text;
    l_flight_id integer;
    l_status text;
    l_scheduled_departure timestamptz;
BEGIN
    l_route_no := evt.payload->>'route_no';

    CALL log_message(1, format('  Open bookings for %s', l_route_no));

    l_status := CASE
      WHEN random() < current_setting('gen.cancel_frac')::float THEN 'Cancelled'
      ELSE 'Scheduled'
    END;

    l_scheduled_departure := evt.at + OPEN_BOOKINGS_INTERVAL();
    INSERT INTO bookings.flights(
      route_no,
      status,
      scheduled_departure,
      scheduled_arrival
    )
    VALUES (
      l_route_no,
      l_status,
      l_scheduled_departure,
      l_scheduled_departure + flight_duration(l_route_no, l_scheduled_departure)
    )
    RETURNING flight_id INTO l_flight_id;

    IF l_status = 'Scheduled' THEN
        INSERT INTO gen.seats_remain(flight_id, available)
        SELECT l_flight_id, count(*)
        FROM bookings.routes r
          JOIN bookings.seats s ON s.airplane_code = r.airplane_code
        WHERE r.route_no = l_route_no
          AND r.validity @> l_scheduled_departure;

        next := l_scheduled_departure - OPEN_REGISTRATION_INTERVAL();
        INSERT INTO gen.events(at,type,payload)
            VALUES (next, 'REGISTRATION',
                    jsonb_object(ARRAY['route_no',l_route_no,'flight_id',l_flight_id::text]));
    END IF;

    next := next_flight(l_route_no, l_scheduled_departure + interval '1 sec') - OPEN_BOOKINGS_INTERVAL();
    -- next flight will not exist at end of the schedule;
    -- it's OK because BUILD ROUTES will seed the events anew
    IF next IS NOT NULL THEN
        INSERT INTO gen.events(at,type,payload)
            VALUES (next, 'FLIGHT', jsonb_object(ARRAY['route_no',l_route_no]));
    END IF;
END;
$$ LANGUAGE plpgsql;

/*
  Open registration for the flight.
*/
CREATE OR REPLACE PROCEDURE registration(evt gen.events)
AS $$
DECLARE
    next timestamptz;
    l_route_no text;
    l_flight_id integer;
    delay interval;
    l_ticket_no text;
BEGIN
    l_route_no := evt.payload->>'route_no';
    l_flight_id := (evt.payload->>'flight_id')::integer;

    CALL log_message(1, format('  Open registration for %s (%s)', l_route_no, l_flight_id));

    IF random() < current_setting('gen.delay_frac')::float THEN
        delay := interval '1 hour' + -- minimum delay
                 interval '5 minute' * rnd_binomial( (12-1)*60/5, 0.2 ); -- up to 12 hours
        UPDATE bookings.flights
        SET status = 'Delayed'
        WHERE flight_id = l_flight_id;
    ELSE
        delay := interval '1 minute' * rnd_binomial(60, 0.05); -- small delay is possible
        UPDATE bookings.flights
        SET status = 'On Time'
        WHERE flight_id = l_flight_id;
    END IF;

    -- generate check-in events for all tickets with no boarding passes
    FOR l_ticket_no IN (
        SELECT s.ticket_no
        FROM bookings.segments s
        WHERE s.flight_id = l_flight_id
          AND NOT EXISTS (
                SELECT NULL FROM bookings.boarding_passes bp WHERE bp.ticket_no = s.ticket_no
              )
    )
    LOOP
        -- Uniform distribution is good enough because check-in time is not recorded in any table.
        next := evt.at + delay +
            (OPEN_REGISTRATION_INTERVAL() - CLOSE_REGISTRATION_INTERVAL()) * random();
        INSERT INTO gen.events(at,type,payload)
            VALUES (next, 'CHECK-IN',
                    jsonb_object(ARRAY['ticket_no',l_ticket_no]));
    END LOOP;

    next := evt.at + delay + OPEN_REGISTRATION_INTERVAL() - OPEN_BOARDING_INTERVAL();
    INSERT INTO gen.events(at,type,payload)
        VALUES (next, 'BOARDING',
                jsonb_object(ARRAY['route_no',l_route_no,'flight_id',l_flight_id::text]));
END;
$$ LANGUAGE plpgsql;

/*
  Check-in
  Issue all boarding passes along the path.
*/
CREATE OR REPLACE PROCEDURE check_in(evt gen.events)
AS $$
DECLARE
    l_flight_id integer;
    l_ticket_no text;
    l_fare_conditions text;
BEGIN
    l_ticket_no := evt.payload->>'ticket_no';

    CALL log_message(2, format('  Check-in %s', l_ticket_no));

    -- create boarding_passes in a fixed order to prevent deadlocks
    FOR l_flight_id, l_fare_conditions IN (
        SELECT s.flight_id, s.fare_conditions
        FROM bookings.segments s
        WHERE s.ticket_no = l_ticket_no
        ORDER BY s.flight_id
    )
    LOOP
        PERFORM pg_advisory_xact_lock(l_flight_id);
        INSERT INTO bookings.boarding_passes(ticket_no, flight_id, seat_no)
        SELECT l_ticket_no, l_flight_id, get_seat_no(l_flight_id, l_fare_conditions)
        -- Duplicate CHECK-IN events are possible if the passenger not yet checked in
        -- when registration for the next flight is already opened.
        -- This check cannot be moved to FOR in Read Committed isolation level.
        WHERE NOT EXISTS (
            SELECT NULL
            FROM bookings.boarding_passes bp
            WHERE bp.ticket_no = l_ticket_no
              AND bp.flight_id = l_flight_id
        );
    END LOOP;
END;
$$ LANGUAGE plpgsql;

/*
  Open boarding for the flight.
*/
CREATE OR REPLACE PROCEDURE boarding(evt gen.events, end_date timestamptz)
AS $$
DECLARE
    next timestamptz;
    takeoff timestamptz;
    l_route_no text;
    l_flight_id integer;
    l_ticket_no text;
BEGIN
    l_route_no := evt.payload->>'route_no';
    l_flight_id := (evt.payload->>'flight_id')::integer;

    CALL log_message(1, format('  Open boarding for %s (%s)', l_route_no, l_flight_id));

    UPDATE bookings.flights
    SET status = 'Boarding'
    WHERE flight_id = l_flight_id;

    takeoff := evt.at + OPEN_BOARDING_INTERVAL() +
        interval '1 minute' * rnd_erlang(5, 0.6); -- small delay is possible (mean 3 min, max ~15)

    IF evt.at + OPEN_BOARDING_INTERVAL() - CLOSE_BOARDING_INTERVAL() < end_date THEN
        -- Optimization: if boarding ends before generation end date, we don't bother
        -- with individual GET IN events and update all boarding passes in one batch.
        -- This is helpful, because otherwise GET IN events take about half the queue.
        UPDATE bookings.boarding_passes bp
        SET boarding_no = tt.boarding_no,
            boarding_time = tt.boarding_time
        FROM (
            SELECT ticket_no, boarding_time, row_number() OVER (ORDER BY boarding_time) boarding_no
            FROM (
                SELECT ticket_no,
                       evt.at + (OPEN_BOARDING_INTERVAL() - CLOSE_BOARDING_INTERVAL()) * random() boarding_time
                  FROM bookings.boarding_passes
                 WHERE flight_id = l_flight_id
                   AND NOT miss_flight(takeoff, ticket_no, l_flight_id)
            ) t
        ) tt
        WHERE bp.ticket_no = tt.ticket_no
          AND bp.flight_id = l_flight_id;
    ELSE
        -- Generation end date is coming, so insert individual GET IN events.
        FOR l_ticket_no IN (
            SELECT ticket_no FROM bookings.boarding_passes WHERE flight_id = l_flight_id
        )
        LOOP
            IF NOT miss_flight(takeoff, l_ticket_no, l_flight_id) THEN
                next := evt.at + (OPEN_BOARDING_INTERVAL() - CLOSE_BOARDING_INTERVAL()) * random();
                INSERT INTO gen.events(at,type,payload)
                    VALUES (next, 'GET IN',
                            jsonb_object(ARRAY['ticket_no',l_ticket_no,'flight_id',l_flight_id::text]));
            END IF;
        END LOOP;
    END IF;

    INSERT INTO gen.events(at,type,payload)
        VALUES (takeoff, 'TAKEOFF',
                jsonb_object(ARRAY['route_no',l_route_no,'flight_id',l_flight_id::text]));
END;
$$ LANGUAGE plpgsql;

/*
  Getting in into the airplane.
*/
CREATE OR REPLACE PROCEDURE get_in(evt gen.events)
AS $$
DECLARE
    l_flight_id integer;
    l_ticket_no text;
BEGIN
    l_ticket_no := evt.payload->>'ticket_no';
    l_flight_id := (evt.payload->>'flight_id')::integer;

    CALL log_message(2, format('  Getting in %s, %s', l_flight_id, l_ticket_no));

    PERFORM pg_advisory_xact_lock(l_flight_id);
    UPDATE bookings.boarding_passes
    SET boarding_no = (
          SELECT count(*) + 1
            FROM bookings.boarding_passes
           WHERE flight_id = l_flight_id
             AND boarding_no IS NOT NULL
        ),
        boarding_time = evt.at
    WHERE ticket_no = l_ticket_no
      AND flight_id = l_flight_id;
END;
$$ LANGUAGE plpgsql;

/*
  Takeoff - start the flight.
*/
CREATE OR REPLACE PROCEDURE takeoff(evt gen.events)
AS $$
DECLARE
    next timestamptz;
    l_route_no text;
    l_flight_id integer;
    l_duration interval;
BEGIN
    l_route_no := evt.payload->>'route_no';
    l_flight_id := (evt.payload->>'flight_id')::integer;

    CALL log_message(1, format('  Takeoff for %s (%s)', l_route_no, l_flight_id));

    UPDATE bookings.flights
    SET status = 'Departed',
        actual_departure = evt.at
    WHERE flight_id = l_flight_id;

    l_duration := flight_duration(l_flight_id) * -- scheduled
        (1.0 + 0.01 * rnd_normal()) + interval '30 second'; -- usually +/- 6%

    next := evt.at + l_duration;
    INSERT INTO gen.events(at,type,payload)
        VALUES (next, 'LANDING',
                jsonb_object(ARRAY['route_no',l_route_no,'flight_id',l_flight_id::text]));
END;
$$ LANGUAGE plpgsql;

/*
  Landing - end the flight.
*/
CREATE OR REPLACE PROCEDURE landing(evt gen.events)
AS $$
DECLARE
    l_route_no text;
    l_flight_id integer;
BEGIN
    l_route_no := evt.payload->>'route_no';
    l_flight_id := (evt.payload->>'flight_id')::integer;

    CALL log_message(1, format('  Landing for %s (%s)', l_route_no, l_flight_id));

    UPDATE bookings.flights
    SET status = 'Arrived',
        actual_arrival = evt.at
    WHERE flight_id = l_flight_id;
END;
$$ LANGUAGE plpgsql;

/*
  Vacuum
*/
CREATE OR REPLACE PROCEDURE vacuum(evt gen.events)
AS $$
DECLARE
    next timestamptz;
BEGIN
    CALL log_message(0, 'Vacuum');

    PERFORM dblink_exec(current_setting('gen.connstr'),'VACUUM ANALYZE');

    next := evt.at + interval '1 week';
    INSERT INTO gen.events(at,type) VALUES (next, 'VACUUM');
END;
$$ LANGUAGE plpgsql;

/*
  Monitoring
*/
CREATE OR REPLACE PROCEDURE monitoring(evt gen.events)
AS $$
DECLARE
    prev_now timestamptz;

    prev_bookings bigint;
    bookings bigint;
    prev_b_forceoneway bigint;
    prev_b_nopath bigint;
    prev_b_noseat bigint;
    book_stats record;

    prev_b_retries bigint;
    bookref_stats record;

    bpasses bigint;
    prev_bpasses bigint;

    next timestamptz;
BEGIN
    prev_now := (evt.payload->>'now')::timestamptz;
    CALL log_message(0, format('%s: one day in %s', evt.at::date, now()-prev_now));

    prev_bookings := (evt.payload->>'bookings')::bigint;
    bookings := (SELECT count(*) FROM bookings.bookings);
    prev_b_forceoneway := (evt.payload->>'b_forceoneway')::bigint;
    prev_b_nopath := (evt.payload->>'b_nopath')::bigint;
    prev_b_noseat := (evt.payload->>'b_noseat')::bigint;
    SELECT success, forceoneway, nopath, noseat INTO book_stats FROM gen.stat_bookings;
    CALL log_message(0, format('New bookings: %s (forceoneway %s), nopath %s, noseat %s',
        bookings - prev_bookings,
        book_stats.forceoneway - prev_b_forceoneway,
        book_stats.nopath - prev_b_nopath,
        book_stats.noseat - prev_b_noseat
    ));

    prev_b_retries := (evt.payload->>'b_retries')::bigint;
    SELECT retries INTO bookref_stats FROM gen.stat_bookrefs;
    IF bookref_stats.retries > prev_b_retries AND bookings > prev_bookings THEN
        CALL log_message(0, format('Book-ref retries = %s (%s retries/booking)',
            bookref_stats.retries - prev_b_retries,
            round((bookref_stats.retries - prev_b_retries)::numeric / (bookings - prev_bookings),3)
        ));
    END IF;

    bpasses := (SELECT count(*) FROM bookings.boarding_passes);
    prev_bpasses := (evt.payload->>'bpasses')::bigint;
    IF bpasses > prev_bpasses THEN
        CALL log_message(0, format('New boarding passes: %s', bpasses - prev_bpasses));
    END IF;

    next := evt.at + interval '1 day';
    INSERT INTO gen.events(at,type,payload)
        VALUES (next, 'MONITORING', json_object(ARRAY[
            'now',now()::text,
            'bookings',bookings::text,
            'bpasses',bpasses::text,
            'b_success',book_stats.success::text,
            'b_forceoneway',book_stats.forceoneway::text,
            'b_nopath',book_stats.nopath::text,
            'b_noseat',book_stats.noseat::text,
            'b_retries',bookref_stats.retries::text]));
END;
$$ LANGUAGE plpgsql;

