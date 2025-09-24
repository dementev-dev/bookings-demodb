/*
Demonstration Database Generator

Copyright (c) 2025 Postgres Professional

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

--
-- Routines related to routes
--

/*
  Get any airport, uniform distribution.
  An airport must have a non-null traffic to participate in routes.
*/
CREATE OR REPLACE FUNCTION any_airport() RETURNS text
AS $$
DECLARE
    rnd float;
    airport_code text;
BEGIN
    rnd := random();
    SELECT t.airport_code
      INTO airport_code
      FROM (
            SELECT a.airport_code,
                   sum(1.0) OVER (ORDER BY a.airport_code) /
                   sum(1.0) OVER () AS cume_dist
              FROM gen.airports_data a
             WHERE a.traffic IS NOT NULL
           ) t
     WHERE t.cume_dist > rnd
     ORDER BY t.cume_dist
     LIMIT 1;
    RETURN airport_code;
END;
$$
LANGUAGE plpgsql;

/*
  Bookings per week
*/
CREATE OR REPLACE FUNCTION week_bookings(traffic float) RETURNS float
AS $$
    SELECT traffic * current_setting('gen.traffic_coeff')::float;
$$ LANGUAGE sql;

/*
  Chance to choose an airport, which is proportional to the traffic of destination airport
  and inversely proportional to the distance.
  The value can be greater than 1. It is converted to probability [0,1] in build_routes() and do_init().
*/
CREATE OR REPLACE FUNCTION chance_to_fly(
    traffic float,
    distance float
) RETURNS float
AS $$
    SELECT traffic / distance;
$$
LANGUAGE sql;

/*
  Get destination airport for a source airport.
  Tries to respect the desired domestic flights fraction, but
  for countries with just one city has to choose a foreign flight.
*/
CREATE OR REPLACE FUNCTION airport_to(
    airport_from text
) RETURNS text
AS $$
DECLARE
    rnd float;
    airport_code text;
    is_domestic boolean;
BEGIN
    rnd := random();
    is_domestic := random() < current_setting('gen.domestic_frac')::float;
    -- data is static, so tables are pre-calculated at INIT stage
    SELECT t.arrival_airport
    INTO airport_code
    FROM gen.airport_to_prob t
    WHERE t.departure_airport = airport_from
      AND (t.domestic IS NULL OR t.domestic = is_domestic)
      AND t.cume_dist > rnd
    ORDER BY t.cume_dist
    LIMIT 1;
    RETURN airport_code;
END;
$$
LANGUAGE plpgsql;

/*
  Add directions between two airports.
  Returns true if directions are new and added, false otherwise.
*/
CREATE OR REPLACE FUNCTION add_direction(
    airport_from char(3),
    airport_to char(3)
) RETURNS boolean
AS $$
DECLARE
    n1 integer;
    n2 integer;
BEGIN
    INSERT INTO gen.directions(departure_airport, arrival_airport)
        VALUES (airport_from, airport_to),
               (airport_to, airport_from)
    ON CONFLICT ON CONSTRAINT directions_departure_airport_arrival_airport_key DO NOTHING;
    IF FOUND THEN
        -- maintain connectedness
        n1 := n FROM gen.directions_connect WHERE airport_code = airport_from;
        n2 := n FROM gen.directions_connect WHERE airport_code = airport_to;
        IF n1 != n2 THEN
            UPDATE gen.directions_connect
            SET n = least(n1,n2)
            WHERE n = greatest(n1,n2);
        END IF;
        RETURN true;
    END IF;
    RETURN false;
END;
$$ LANGUAGE plpgsql;

/*
  Estimated number of passengers per week, flying from one airport to another
  directly connected airport.
*/
CREATE OR REPLACE FUNCTION traffic_per_route(
    airport_from char(3),
    airport_to char(3)
) RETURNS float
AS $$
    SELECT traffic
    FROM gen.week_traffic
    WHERE departure_airport = airport_from
      AND arrival_airport = airport_to;
$$ LANGUAGE sql;

/*
  Choose aircraft and frequency of flights for the given pair of airports.
*/
CREATE OR REPLACE FUNCTION suitable_aircraft(
    airport_from char(3),
    airport_to char(3),
    week_traffic float,
    airplane_code OUT char(3),
    n_days_of_week OUT integer
)
AS $$
BEGIN
    IF week_traffic = 0.0 THEN
        week_traffic := 1.0; -- to choose smallest possible aircraft
    END IF;
    week_traffic := week_traffic * 1.15; -- a chance to enlarge capacity for overfilled routes
    WITH f(airplane_code, n_days_of_week, fill) AS (
      SELECT ac.airplane_code, dw.n_days_of_week, week_traffic / (ac.seats * dw.n_days_of_week)
      FROM (
            SELECT ac.airplane_code, count(*) AS seats
            FROM gen.airplanes_data ac
              JOIN gen.seats s ON s.airplane_code = ac.airplane_code
            WHERE ac.in_use
              AND ac.range >= ( -- enough range
                SELECT (a_from.coordinates <@> a_to.coordinates) * 1.609344
                FROM gen.airports_data a_from, gen.airports_data a_to
                WHERE a_from.airport_code = airport_from
                AND a_to.airport_code = airport_to
              )
            GROUP BY ac.airplane_code
           ) ac
        CROSS JOIN (VALUES (1),(2),(3),(4),(5),(6),(7)) dw(n_days_of_week)
    )
    SELECT f.airplane_code, f.n_days_of_week
    INTO airplane_code, n_days_of_week
    FROM f
    -- optimize waste but allow overfill when no other options
    ORDER BY abs(1.0 - f.fill) * CASE WHEN f.fill > 1.0 THEN 100.0 ELSE 1.0 END
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

/*
  Create a days-of-week pattern
  (1 = Mon, 7 = Sun)
*/
CREATE OR REPLACE FUNCTION dow_pattern(n_days_of_week integer)
RETURNS integer[]
AS $$
DECLARE
    f float;
    d integer;
    dow integer[];
BEGIN
    ASSERT n_days_of_week BETWEEN 1 AND 7, 'n_days_of_week out of range';
    f := random() * 7;
    d := floor(f);
    FOR i IN 1 .. n_days_of_week LOOP
        dow[i] := mod(d, 7) + 1;
        f := f + 7.0 / n_days_of_week;
        d := greatest(floor(f)::integer, d+1);
    END LOOP;
    RETURN ARRAY(SELECT unnest(dow) d ORDER BY 1);
END;
$$ LANGUAGE plpgsql;

/*
  Estimated duration of the flight.
*/
CREATE OR REPLACE FUNCTION estimated_duration(
    airport_from char(3),
    airport_to char(3),
    airplane_code char(3)
)
RETURNS interval
AS $$
    WITH d AS (
      -- never fly exactly by great circle, so increase the distance by the factor of 1.1
      SELECT (a_from.coordinates <@> a_to.coordinates) * 1.609344 * 1.1 distance
      FROM gen.airports_data a_from, gen.airports_data a_to
      WHERE a_from.airport_code = airport_from
        AND a_to.airport_code = airport_to
    )
    SELECT interval '5 minute' * round(60.0/5 * d.distance / ac.speed) +
           interval '30 min' -- simple account for taxi etc.
    FROM d, gen.airplanes_data ac
    WHERE ac.airplane_code = estimated_duration.airplane_code;
$$ LANGUAGE sql;

/*
  Add a route between two airports.
  The added route retains route number and scheduled time from the previous route,
  if any. Otherwise, the new route is assigned a new flight_id based on rno.
  Returns next rno to use.
*/
CREATE OR REPLACE function add_route(
    rno integer,
    airport_from char(3),
    airport_to char(3),
    validity tstzrange
) RETURNS integer
AS $$
DECLARE
    traffic_from float;
    traffic_to float;
    week_traffic float;
    airplane_code char(3);
    n_days_of_week integer;
    l_route_no text;
    l_scheduled_time time;
BEGIN
    -- estimate number of passengers per week (traffic)
    traffic_from := traffic_per_route(airport_from, airport_to);
    traffic_to := traffic_per_route(airport_to, airport_from);
    -- we want (arguably) the same aircraft to fly to and from
    week_traffic := (traffic_from + traffic_to)/2.0; -- greatest may lead to half-empty routes

    -- abandon entirely empty routes
    IF week_traffic = 0.0 THEN
        RETURN rno;
    END IF;

    SELECT *
    FROM suitable_aircraft(airport_from, airport_to, week_traffic)
    INTO airplane_code, n_days_of_week;

    IF airplane_code IS NULL THEN
        CALL log_message(0, format('Cannot choose an aircraft for %s -> %s (out of range?)', airport_from, airport_to));
        RETURN rno;
    ELSE
        CALL log_message(0, format('%s -> %s: %s x %s, traffic = %s pass/week',airport_from, airport_to, airplane_code, n_days_of_week, round(week_traffic)));
    END IF;

    SELECT route_no, scheduled_time
    INTO l_route_no, l_scheduled_time
    FROM bookings.routes
    WHERE departure_airport = airport_from
      AND arrival_airport = airport_to
    LIMIT 1;

    IF l_route_no IS NULL THEN -- no previous route
        l_route_no := current_setting('gen.airlines_code')||lpad(rno::text,4,'0');
        rno := rno + 1;
        -- simple uniform random (can we invent something better?)
        l_scheduled_time := rnd_uniform('00:00:00'::time, '23:59:59'::time);
    END IF;

    INSERT INTO bookings.routes(
        route_no,
        departure_airport,
        arrival_airport,
        validity,
        airplane_code,
        days_of_week,
        scheduled_time,
        duration
    )
    VALUES (
        l_route_no,
        airport_from,
        airport_to,
        validity,
        airplane_code,
        dow_pattern(n_days_of_week),
        l_scheduled_time,
        estimated_duration(airport_from, airport_to, airplane_code)
    );

    RETURN rno;
END;
$$ LANGUAGE plpgsql;

/*
  Build a connected graph of airlines.
*/
CREATE OR REPLACE PROCEDURE build_routes(evt gen.events)
AS $$
DECLARE
    start_date timestamptz;
    period integer;
    ts_from timestamptz;
    ts_to timestamptz;
    next timestamptz;
    route_no text;
    validity tstzrange;
    airport_from char(3);
    airport_to char(3);
    rno integer;
    try integer;
    max_tries constant integer := 100;
BEGIN
    -- we can't rely on evt.at to determine start of the next validity period, because
    -- of date arithmetic: 31-Mar + 1 month = 30-Apr (not to mention February)
    start_date := (evt.payload->>'start')::timestamptz;
    period := (evt.payload->>'period')::integer;
    -- routes will take effect in two months
    ts_from := start_date + ROUTES_DURATION()*period;
    -- rebuild routes from scratch once a month
    -- (not the same as ts_from + ROUTES_DURATION()!)
    ts_to := start_date + ROUTES_DURATION()*(period+1);
    validity := tstzrange(ts_from, ts_to);

    CALL log_message(0, format('Building routes, range %s', validity));

    -- we always build routes in a single process, no worries about locks
    TRUNCATE TABLE gen.directions;
    TRUNCATE TABLE gen.directions_connect;

    -- initialize table for graph connectedness check with unique numbers for each airport
    INSERT INTO gen.directions_connect(airport_code,n)
      SELECT airport_code, row_number() OVER (ORDER BY airport_code) -- order doesn't matter
      FROM gen.airports_data
      WHERE traffic IS NOT NULL;

    -- for all airports, starting from the least busy one,
    -- add directions to two other random airports
    FOR airport_from IN (
        SELECT airport_code FROM gen.airports_data
        WHERE traffic IS NOT NULL
        ORDER BY traffic
    )
    LOOP
        try := 1;
        LOOP
            airport_to := airport_to(airport_from);
            EXIT WHEN add_direction(airport_from, airport_to) OR try >= max_tries;
            try := try + 1;
        END LOOP;
        try := 1;
        LOOP
            airport_to := airport_to(airport_from);
            EXIT WHEN add_direction(airport_from, airport_to) OR try >= max_tries;
            try := try + 1;
        END LOOP;
    END LOOP;

    -- add extra directions until the graph is connected
    try := 1;
    WHILE (SELECT count(DISTINCT n) FROM gen.directions_connect) > 1 AND try <= max_tries
    LOOP
        airport_from := any_airport();
        airport_to := airport_to(airport_from);
        PERFORM add_direction(airport_from, airport_to);
        try := try + 1;
    END LOOP;
    IF try > max_tries THEN
        RAISE EXCEPTION 'Cannot build connected graph after % tries', max_tries;
    END IF;

    -- estimate passengers traffic by routes
    -- (by dividing total traffic of the departure airport between possible directions
    -- proportionally to traffics of the arrival airports)
    TRUNCATE TABLE gen.week_traffic;
    INSERT INTO gen.week_traffic(departure_airport, arrival_airport, traffic)
      WITH a AS (
        SELECT d.departure_airport,
               d.arrival_airport,
               a_from.traffic,
               chance_to_fly(a.traffic, a_from.coordinates <@> a.coordinates) * CASE
                   WHEN a_from.country_code = a.country_code
                     THEN current_setting('gen.domestic_frac')::float
                   ELSE 1.0 - current_setting('gen.domestic_frac')::float
                 END AS pp
        FROM gen.airports_data a_from
          JOIN gen.directions d ON d.departure_airport = a_from.airport_code
          JOIN gen.airports_data a ON a.airport_code = d.arrival_airport
      ), b AS (
        SELECT departure_airport, arrival_airport, traffic,
               pp / sum(pp) OVER (PARTITION BY departure_airport) AS p
        FROM a
      )
      SELECT departure_airport, arrival_airport,
             week_bookings( traffic * p ) *
              -- average number of passengers per one booking
             ((current_setting('gen.max_pass_per_booking')::integer - 1) * 0.1 + 1.0) *
             -- roundtrip passangers
             (1.0 + current_setting('gen.roundtrip_frac')::float)
      FROM b;

    -- now transform the generated directions into routes,
    -- taking into account passengers traffic
    SELECT count(*) + 1
    INTO rno
    FROM (SELECT DISTINCT departure_airport, arrival_airport FROM bookings.routes) t;

    FOR airport_from, airport_to IN (
        SELECT departure_airport, arrival_airport FROM gen.directions
    ) LOOP
        rno := add_route(rno, airport_from, airport_to, validity);
    END LOOP;

    -- next rebuild event
    INSERT INTO gen.events(at,type,payload)
        VALUES (evt.at + ROUTES_DURATION(), 'BUILD ROUTES',
            jsonb_object(ARRAY['start',start_date::text,'period',(period+1)::text]));

    -- seed flights: open for booking in advance
    FOR route_no IN (
        SELECT r.route_no
        FROM bookings.routes r
        WHERE r.validity @> ts_from
    )
    LOOP
        next := next_flight(route_no, ts_from) - OPEN_BOOKINGS_INTERVAL();
        INSERT INTO gen.events(at,type,payload)
            VALUES (next, 'FLIGHT', jsonb_object(ARRAY['route_no',route_no]));
    END LOOP;

END;
$$
LANGUAGE plpgsql;

/*
  Get the nearest flight according to the schedule, starting from the specifited timestamp.
*/
CREATE OR REPLACE FUNCTION next_flight(route_no text, ts timestamptz)
RETURNS timestamptz
AS $$
    WITH dates AS (
      SELECT
        (date_trunc('day', g AT TIME ZONE a.timezone) + r.scheduled_time) AT TIME ZONE a.timezone d, -- ts with timezone
        extract(isodow FROM date_trunc('day', g AT TIME ZONE a.timezone) + r.scheduled_time)::integer dow, -- in airport time zone
        r.validity,
        r.days_of_week
      FROM bookings.routes r
        JOIN bookings.airports_data a ON a.airport_code = r.departure_airport
        CROSS JOIN generate_series(lower(r.validity), upper(r.validity), interval '1 day') g
      WHERE r.validity @> next_flight.ts
        AND r.route_no = next_flight.route_no
    )
    SELECT d
    FROM dates
    WHERE d <@ validity
      AND ARRAY[dow] && days_of_week
      AND d >= next_flight.ts
    ORDER BY d
    LIMIT 1;
$$ LANGUAGE sql;

/*
   Get duration of the flight (two overloaded versions).
*/
CREATE OR REPLACE FUNCTION flight_duration(flight_id integer)
RETURNS interval
AS $$
    SELECT r.duration
    FROM bookings.flights f
      JOIN bookings.routes r ON r.route_no = f.route_no
    WHERE f.flight_id = flight_duration.flight_id
      AND r.validity @> f.scheduled_departure;
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION flight_duration(route_no text, at timestamptz)
RETURNS interval
AS $$
    SELECT r.duration
    FROM bookings.routes r
    WHERE r.route_no = flight_duration.route_no
      AND r.validity @> flight_duration.at;
$$ LANGUAGE sql;

