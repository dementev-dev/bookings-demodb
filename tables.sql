/*
Demonstration Database Generator

Copyright (c) 2025 Postgres Professional

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

--
-- gen schema
-- Used for generation internal data
--

DROP SCHEMA IF EXISTS gen CASCADE;
CREATE SCHEMA gen;

/*
   Airplanes
*/
CREATE UNLOGGED TABLE gen.airplanes_data (
    airplane_code char(3) NOT NULL,
    model         jsonb   NOT NULL,
    range         integer NOT NULL,
    speed         integer NOT NULL,
    in_use        boolean NOT NULL
);

\copy gen.airplanes_data FROM 'airplanes_data.dat'

/*
   Seats
*/
CREATE UNLOGGED TABLE gen.seats (
    airplane_code   char(3) NOT NULL,
    seat_no         text    NOT NULL,
    fare_conditions text    NOT NULL
);

\copy gen.seats FROM 'seats.dat'

/*
  Remaining number of seats to quickly skip all-booked flights
*/
CREATE UNLOGGED TABLE gen.seats_remain (
    flight_id integer PRIMARY KEY,
    available integer NOT NULL,
    CHECK (available >= 0)
);

/*
   Airports
   Those with not null traffic will participate in routes.

   For export:
   \copy (SELECT * FROM gen.airports_data ORDER BY airport_code) to airport_data.dat
*/
CREATE UNLOGGED TABLE gen.airports_data (
    airport_code char(3) PRIMARY KEY,
    airport_name jsonb   NOT NULL,
    city         jsonb   NOT NULL,
    country_code char(2) NOT NULL,
    country      jsonb   NOT NULL,
    coordinates  point   NOT NULL,
    timezone     text    NOT NULL,
    traffic      numeric
);

\copy gen.airports_data FROM 'airports_data.dat'

/*
   Directions graph
   Generated directions transform into particular routes.
*/
CREATE UNLOGGED TABLE gen.directions (
    departure_airport char(3) NOT NULL,
    arrival_airport   char(3) NOT NULL,
    UNIQUE (departure_airport, arrival_airport)
);

/*
   Table to maintain connectedness of directions graph.
*/
CREATE UNLOGGED TABLE gen.directions_connect (
    airport_code char(3) NOT NULL,
    n            integer NOT NULL
);

/*
  Table with pre-calculated probabilities to choose an airport.
  Populated during the INIT stage.
*/
CREATE UNLOGGED TABLE gen.airport_to_prob (
    departure_airport char(3) NOT NULL,
    arrival_airport   char(3) NOT NULL,
    domestic          boolean, -- NULL for countires with just one city
    cume_dist         float   NOT NULL
);
CREATE INDEX ON gen.airport_to_prob (departure_airport, domestic, cume_dist, arrival_airport);

/*
  Week traffic between two directly connected airports.
  This table is populated in the build_routes() function by estimated traffic and
  is used inside this function only.
*/
CREATE UNLOGGED TABLE gen.week_traffic (
    departure_airport char(3) NOT NULL,
    arrival_airport   char(3) NOT NULL,
    traffic           float
);

/*
   First (given) names distribution per country.

   The grp column is intended primarily for gender-specific family names.
   In this case you have to provide two sets of given names and family
   names for both genders (say, values M and F). It may be inconvenient
   for countries where most family names are gender-neutral with some exceptions
   (like India), but that's the way it works.

   You can further divide names into groups by choosing different values
   to avoid e.g. interfaith names like Mohammed Singh etc.

   For countries with gender-neutral family names (regardless
   gender-neutrality of given names), values in both firstname and
   lastname tables must be set to the same char.

   Field cume_dist is calculated automatically by generation engine.

   For export after changes:
   UPDATE gen.firstnames SET cume_dist = NULL; -- for nice diff
   \copy (SELECT * FROM gen.firstnames ORDER BY country,grp,name,qty) to firstnames.dat

   Normalization (not needed, just for aestetics):
   CREATE TABLE gen.lastnames0 AS
      SELECT country, grp, name, round(qty*( 1000000.0 / sum(qty) OVER (PARTITION BY country, grp) )) qty
      FROM gen.lastnames;
   UPDATE gen.lastnames0 fn SET qty = fn.qty + delta
   FROM (
        SELECT * FROM (
            SELECT country, grp, name, qty,
                row_number() OVER (PARTITION BY country, grp ORDER BY qty desc) r,
                1000000 - sum(qty) OVER (PARTITION BY country, grp) delta
            FROM gen.lastnames0
        )
        WHERE r = 1
   ) t
   WHERE fn.country=t.country AND fn.grp= t.grp AND fn.name = t.name;
*/
CREATE UNLOGGED TABLE gen.firstnames (
    country   text    NOT NULL,
    grp       text    NOT NULL,
    name      text    NOT NULL,
    qty       integer NOT NULL,
    cume_dist float,
    PRIMARY KEY (country,grp,name)
);

\copy gen.firstnames FROM 'firstnames.dat'

/*
   Last (family) names distribution per country.

   Field cume_dist is calculated automatically by generation engine.

   For export after changes:
   UPDATE gen.lastnames SET cume_dist = NULL; -- for nice diff
   \copy (SELECT * FROM gen.lastnames ORDER BY country,grp,name,qty) to lastnames.dat
*/
CREATE UNLOGGED TABLE gen.lastnames (
    country   text    NOT NULL,
    grp       text    NOT NULL,
    name      text    NOT NULL,
    qty       integer NOT NULL,
    cume_dist float,
    PRIMARY KEY (country,grp,name)
);

\copy gen.lastnames FROM 'lastnames.dat'

/*
  List of unique passengers.
  Passenger with the same ID always has the same name.
*/
CREATE UNLOGGED TABLE gen.passengers (
    id           text        PRIMARY KEY,
    name         text        NOT NULL,
    locked_until timestamptz NOT NULL -- do not use this passenger for some time
);

/*
  Event queue for the generation engine.
*/
CREATE UNLOGGED TABLE gen.events (
    event_id bigint      PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    at       timestamptz NOT NULL,
    type     text        NOT NULL,
    payload  jsonb       DEFAULT NULL,
    pid      integer     DEFAULT NULL
);
CREATE INDEX ON gen.events(at);

/*
  Event queue history, useful for debugging.
*/
CREATE UNLOGGED TABLE gen.events_history (
    LIKE gen.events
);

/*
  Track misses for flights.
*/
CREATE UNLOGGED TABLE gen.missed_flights (
    ticket_no text PRIMARY KEY
);

/*
  Monitoring stats counters per bookings.
*/
CREATE UNLOGGED TABLE gen.stat_bookings (
    success     bigint NOT NULL, -- total successful bookings
    forceoneway bigint NOT NULL, -- tickets converted to one-way due to no roundtrip path
    nopath      bigint NOT NULL, -- bookings cancelled due to no suitable path
    noseat      bigint NOT NULL  -- bookings cancelled due to cabin overfill
);

/*
  Monitoring stats counters per job.
*/
CREATE UNLOGGED TABLE gen.stat_jobs (
    pid    integer PRIMARY KEY,
    events bigint  NOT NULL -- events processed
);

/*
  Monitoring stats for book_ref retries.
*/
CREATE UNLOGGED TABLE gen.stat_bookrefs (
    retries integer
);

/*
  Logging table.
  We use background processes to process the queue, so we can't simply
  put messages to console.

  Use command like
    SELECT * FROM gen.log ORDER BY at DESC LIMIT 20 \watch 30
  to monitor the process.
*/
CREATE UNLOGGED TABLE gen.log (
    at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    pid      integer     NOT NULL DEFAULT pg_backend_pid(),
    severity integer     NOT NULL,
    message  text        NOT NULL
);

CREATE SEQUENCE gen.ticket_s START WITH 5432000000;

--
-- bookings schema
--

DROP SCHEMA IF EXISTS bookings CASCADE;
CREATE SCHEMA bookings;

/*
  Language code for translatable names
*/
CREATE FUNCTION bookings.lang() RETURNS text
LANGUAGE sql STABLE
RETURN current_setting('bookings.lang',true);

COMMENT ON FUNCTION bookings.lang() IS 'Language code for translatable names';

/*
  Timestamp of backup, i.e. "current" moment for the generated data.
  This function is re-created by the generation procedure.
*/
CREATE FUNCTION bookings.now() RETURNS timestamptz
LANGUAGE sql IMMUTABLE
RETURN NULL::timestamptz;

COMMENT ON FUNCTION bookings.now() IS 'Current moment for the generated data';

CREATE UNLOGGED TABLE bookings.airplanes_data (
    airplane_code char(3) PRIMARY KEY,
    model         jsonb   NOT NULL,
    range         integer NOT NULL,
    speed         integer NOT NULL,
    CHECK ((range > 0)),
    CHECK ((speed > 0))
);

COMMENT ON TABLE bookings.airplanes_data IS 'Airplanes (internal multilingual data)';
COMMENT ON COLUMN bookings.airplanes_data.airplane_code IS 'Airplane code, IATA';
COMMENT ON COLUMN bookings.airplanes_data.model IS 'Airplane model';
COMMENT ON COLUMN bookings.airplanes_data.range IS 'Maximum flight range, km';
COMMENT ON COLUMN bookings.airplanes_data.speed IS 'Cruise speed, km/h';

CREATE VIEW bookings.airplanes AS
    SELECT ml.airplane_code,
        (ml.model ->> bookings.lang()) AS model,
        ml.range,
        ml.speed
    FROM bookings.airplanes_data ml;

COMMENT ON VIEW bookings.airplanes IS 'Airplanes';
COMMENT ON COLUMN bookings.airplanes.airplane_code IS 'Airplane code, IATA';
COMMENT ON COLUMN bookings.airplanes.model IS 'Airplane model';
COMMENT ON COLUMN bookings.airplanes.range IS 'Maximum flight range, km';
COMMENT ON COLUMN bookings.airplanes.speed IS 'Cruise speed, km/h';

CREATE UNLOGGED TABLE bookings.airports_data (
    airport_code char(3) PRIMARY KEY,
    airport_name jsonb   NOT NULL,
    city         jsonb   NOT NULL,
    country      jsonb   NOT NULL,
    coordinates  point   NOT NULL,
    timezone     text    NOT NULL
);

COMMENT ON TABLE bookings.airports_data IS 'Airports (internal multilingual data)';
COMMENT ON COLUMN bookings.airports_data.airport_code IS 'Airport code, IATA';
COMMENT ON COLUMN bookings.airports_data.airport_name IS 'Airport name';
COMMENT ON COLUMN bookings.airports_data.city IS 'City';
COMMENT ON COLUMN bookings.airports_data.country IS 'Country';
COMMENT ON COLUMN bookings.airports_data.coordinates IS 'Airport coordinates (longitude and latitude)';
COMMENT ON COLUMN bookings.airports_data.timezone IS 'Airport time zone';

CREATE VIEW bookings.airports AS
    SELECT ml.airport_code,
        (ml.airport_name ->> bookings.lang()) AS airport_name,
        (ml.city ->> bookings.lang()) AS city,
        (ml.country ->> bookings.lang()) AS country,
        ml.coordinates,
        ml.timezone
    FROM bookings.airports_data ml;

COMMENT ON VIEW bookings.airports IS 'Airports';
COMMENT ON COLUMN bookings.airports.airport_code IS 'Airport code, IATA';
COMMENT ON COLUMN bookings.airports.airport_name IS 'Airport name';
COMMENT ON COLUMN bookings.airports.city IS 'City';
COMMENT ON COLUMN bookings.airports.country IS 'Country';
COMMENT ON COLUMN bookings.airports.coordinates IS 'Airport coordinates (longitude and latitude)';
COMMENT ON COLUMN bookings.airports.timezone IS 'Airport time zone';


CREATE UNLOGGED TABLE bookings.bookings (
    book_ref     char(6)       PRIMARY KEY,
    book_date    timestamptz   NOT NULL,
    total_amount numeric(10,2) NOT NULL
);

COMMENT ON TABLE bookings.bookings IS 'Bookings';
COMMENT ON COLUMN bookings.bookings.book_ref IS 'Booking number';
COMMENT ON COLUMN bookings.bookings.book_date IS 'Booking date';
COMMENT ON COLUMN bookings.bookings.total_amount IS 'Total booking amount';

CREATE UNLOGGED TABLE bookings.routes (
    route_no          text      NOT NULL,
    validity          tstzrange NOT NULL,
    -- TODO: waiting for PG18
    -- PRIMARY KEY (route_no, validity WITHOUT OVERLAPS)
    EXCLUDE USING gist(route_no with =, validity with &&),
    departure_airport char(3)   NOT NULL REFERENCES bookings.airports_data(airport_code),
    arrival_airport   char(3)   NOT NULL REFERENCES bookings.airports_data(airport_code),
    airplane_code     char(3)   NOT NULL REFERENCES bookings.airplanes_data(airplane_code),
    days_of_week      integer[] NOT NULL,
    scheduled_time    time      NOT NULL,
    duration          interval  NOT NULL
);
COMMENT ON TABLE bookings.routes IS 'Routes';
COMMENT ON COLUMN bookings.routes.route_no IS 'Route number';
COMMENT ON COLUMN bookings.routes.departure_airport IS 'Airport of departure';
COMMENT ON COLUMN bookings.routes.arrival_airport IS 'Airport of arrival';
COMMENT ON COLUMN bookings.routes.validity IS 'Period of validity';
COMMENT ON COLUMN bookings.routes.airplane_code IS 'Airplane code, IATA';
COMMENT ON COLUMN bookings.routes.days_of_week IS 'Days of week array';
COMMENT ON COLUMN bookings.routes.scheduled_time IS 'Scheduled local time of departure';
COMMENT ON COLUMN bookings.routes.duration IS 'Estimated duration';

-- speeds up get_path()
CREATE INDEX ON bookings.routes (departure_airport, lower(validity));

CREATE UNLOGGED TABLE bookings.seats (
    airplane_code   char(3) NOT NULL REFERENCES bookings.airplanes_data(airplane_code) ON DELETE CASCADE,
    seat_no         text    NOT NULL,
    fare_conditions text    NOT NULL,
    PRIMARY KEY (airplane_code, seat_no),
    CONSTRAINT seat_fare_conditions_check
        CHECK ((fare_conditions = ANY (ARRAY['Economy', 'Comfort', 'Business'])))
);

COMMENT ON TABLE bookings.seats IS 'Seats';
COMMENT ON COLUMN bookings.seats.airplane_code IS 'Airplane code, IATA';
COMMENT ON COLUMN bookings.seats.seat_no IS 'Seat number';
COMMENT ON COLUMN bookings.seats.fare_conditions IS 'Travel class';

CREATE UNLOGGED TABLE bookings.tickets (
    ticket_no      text    PRIMARY KEY,
    book_ref       char(6) NOT NULL REFERENCES bookings.bookings(book_ref),
    passenger_id   text    NOT NULL,
    passenger_name text    NOT NULL,
    outbound       boolean NOT NULL,
    UNIQUE (book_ref, passenger_id, outbound)
);

COMMENT ON TABLE bookings.tickets IS 'Tickets';
COMMENT ON COLUMN bookings.tickets.ticket_no IS 'Ticket number';
COMMENT ON COLUMN bookings.tickets.book_ref IS 'Booking number';
COMMENT ON COLUMN bookings.tickets.passenger_id IS 'Passenger ID';
COMMENT ON COLUMN bookings.tickets.passenger_name IS 'Passenger name';
COMMENT ON COLUMN bookings.tickets.outbound IS 'Outbound flight?';

CREATE UNLOGGED TABLE bookings.flights (
    flight_id           integer     PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    route_no            text        NOT NULL,
    status              text        NOT NULL,
    scheduled_departure timestamptz NOT NULL,
    scheduled_arrival   timestamptz NOT NULL,
    actual_departure    timestamptz,
    actual_arrival      timestamptz,
    UNIQUE (route_no, scheduled_departure),
    -- TODO waiting for PG18
    -- FOREIGN KEY (route_no, PERIOD scheduled_departure) REFERENCES booking.routes(route_no, PERIOD validity)
    CONSTRAINT flight_scheduled_check
        CHECK ((scheduled_arrival > scheduled_departure)),
    CONSTRAINT flight_actual_check
        CHECK (((actual_arrival IS NULL) OR ((actual_departure IS NOT NULL) AND (actual_arrival IS NOT NULL) AND (actual_arrival > actual_departure)))),
    CONSTRAINT flight_status_check
        CHECK ((status = ANY (ARRAY['Scheduled', 'On Time', 'Delayed', 'Boarding', 'Departed', 'Arrived', 'Cancelled'])))
);

COMMENT ON TABLE bookings.flights IS 'Flights';
COMMENT ON COLUMN bookings.flights.flight_id IS 'Flight ID';
COMMENT ON COLUMN bookings.flights.route_no IS 'Route number';
COMMENT ON COLUMN bookings.flights.status IS 'Flight status';
COMMENT ON COLUMN bookings.flights.scheduled_departure IS 'Scheduled departure time';
COMMENT ON COLUMN bookings.flights.scheduled_arrival IS 'Scheduled arrival time';
COMMENT ON COLUMN bookings.flights.actual_departure IS 'Actual departure time';
COMMENT ON COLUMN bookings.flights.actual_arrival IS 'Actual arrival time';

CREATE UNLOGGED TABLE bookings.segments (
    ticket_no       text          NOT NULL REFERENCES bookings.tickets(ticket_no),
    flight_id       integer       NOT NULL REFERENCES bookings.flights(flight_id),
    fare_conditions text          NOT NULL,
    price           numeric(10,2) NOT NULL,
    PRIMARY KEY (ticket_no, flight_id),
    CHECK ((price >= (0)::numeric)),
    CHECK ((fare_conditions = ANY (ARRAY['Economy', 'Comfort', 'Business'])))
);

CREATE INDEX ON bookings.segments (flight_id);

COMMENT ON TABLE bookings.segments IS 'Flight segment (leg)';
COMMENT ON COLUMN bookings.segments.ticket_no IS 'Ticket number';
COMMENT ON COLUMN bookings.segments.flight_id IS 'Flight ID';
COMMENT ON COLUMN bookings.segments.fare_conditions IS 'Travel class';
COMMENT ON COLUMN bookings.segments.price IS 'Travel price';

CREATE UNLOGGED TABLE bookings.boarding_passes (
    ticket_no     text    NOT NULL,
    flight_id     integer NOT NULL,
    seat_no       text    NOT NULL,
    boarding_no   integer,
    boarding_time timestamptz,
    PRIMARY KEY (ticket_no, flight_id),
    FOREIGN KEY (ticket_no, flight_id) REFERENCES bookings.segments(ticket_no, flight_id),
    UNIQUE (flight_id, boarding_no),
    UNIQUE (flight_id, seat_no)
);

COMMENT ON TABLE bookings.boarding_passes IS 'Boarding passes';
COMMENT ON COLUMN bookings.boarding_passes.ticket_no IS 'Ticket number';
COMMENT ON COLUMN bookings.boarding_passes.flight_id IS 'Flight ID';
COMMENT ON COLUMN bookings.boarding_passes.boarding_no IS 'Boarding pass number';
COMMENT ON COLUMN bookings.boarding_passes.boarding_time IS 'Boarding time';
COMMENT ON COLUMN bookings.boarding_passes.seat_no IS 'Seat number';

CREATE VIEW bookings.timetable (
    flight_id,
    route_no,
    departure_airport,
    arrival_airport,
    status,
    airplane_code,
    scheduled_departure,
    scheduled_departure_local,
    actual_departure,
    actual_departure_local,
    scheduled_arrival,
    scheduled_arrival_local,
    actual_arrival,
    actual_arrival_local
) AS
SELECT
    f.flight_id,
    f.route_no,
    r.departure_airport,
    r.arrival_airport,
    f.status,
    r.airplane_code,
    f.scheduled_departure,
    f.scheduled_departure AT TIME ZONE dep.timezone AS scheduled_departure_local,
    f.actual_departure,
    f.actual_departure AT TIME ZONE dep.timezone AS actual_departure_local,
    f.scheduled_arrival,
    f.scheduled_arrival AT TIME ZONE arr.timezone AS scheduled_arrival_local,
    f.actual_arrival,
    f.actual_arrival AT TIME ZONE arr.timezone AS actual_arrival_local
FROM bookings.flights f
  JOIN bookings.routes r ON r.route_no = f.route_no AND r.validity @> f.scheduled_departure
  JOIN bookings.airports_data dep ON dep.airport_code = r.departure_airport
  JOIN bookings.airports_data arr ON arr.airport_code = r.arrival_airport;

COMMENT ON VIEW bookings.timetable IS 'Detailed info about flights';
COMMENT ON COLUMN bookings.timetable.flight_id IS 'Flight ID';
COMMENT ON COLUMN bookings.timetable.route_no IS 'Route number';
COMMENT ON COLUMN bookings.timetable.departure_airport IS 'Airport of departure';
COMMENT ON COLUMN bookings.timetable.arrival_airport IS 'Airport of arrival';
COMMENT ON COLUMN bookings.timetable.status IS 'Flight status';
COMMENT ON COLUMN bookings.timetable.airplane_code IS 'Airplane code, IATA';
COMMENT ON COLUMN bookings.timetable.scheduled_departure IS 'Scheduled departure time';
COMMENT ON COLUMN bookings.timetable.scheduled_departure_local IS 'Scheduled departure time in airport''s timezone';
COMMENT ON COLUMN bookings.timetable.actual_departure IS 'Actual departure time';
COMMENT ON COLUMN bookings.timetable.actual_departure_local IS 'Actual departure time in airport''s timezone';
COMMENT ON COLUMN bookings.timetable.scheduled_arrival IS 'Scheduled arrival time';
COMMENT ON COLUMN bookings.timetable.scheduled_arrival_local IS 'Scheduled arrival time in airport''s timezone';
COMMENT ON COLUMN bookings.timetable.actual_arrival IS 'Actual arrival time';
COMMENT ON COLUMN bookings.timetable.actual_arrival_local IS 'Actual arrival time in airport''s timezone';

