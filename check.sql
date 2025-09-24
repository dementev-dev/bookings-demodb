/*
Demonstration Database Generator

Copyright (c) 2025 Postgres Professional

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

--
-- Post-generation queries
--

\echo === Generation errors in log
\echo
-------------------------------------------------------------------------------

SELECT count(*) errors
FROM gen.log
WHERE message LIKE 'Error%';

\echo === Generation stats
\echo
-------------------------------------------------------------------------------

SELECT count(*) num_bookings, min(book_date) book_date_from, max(book_date) book_date_to
FROM bookings.bookings;

SELECT count(*) num_tickets
FROM bookings.tickets;

SELECT count(*) num_segments
FROM bookings.segments;

SELECT count(*) num_flights
FROM bookings.flights;

SELECT count(*) num_routes, count(DISTINCT validity) num_validity_ranges
FROM bookings.routes;

\echo === Generation speed
\echo
-------------------------------------------------------------------------------

SELECT round(
        (SELECT count(*) FROM gen.events_history) /
        (SELECT extract('epoch' FROM max(at)-min(at)) FROM gen.log)
    ) events_per_sec;

\echo === Airplanes utilization
\echo
-------------------------------------------------------------------------------

SELECT avg(ratio) avg_fill_ratio
FROM (
    SELECT route_no, scheduled_departure, flight_id, cabin, booked, 1.0*booked/cabin as ratio
    FROM (
    SELECT f.route_no, f.scheduled_departure, f.flight_id, count(*) cabin,
          (
            SELECT count(*)
            FROM bookings.segments s
            WHERE s.flight_id = f.flight_id
          ) booked
        FROM bookings.flights f
          JOIN bookings.routes r ON r.validity @> f.scheduled_departure
                               AND r.route_no = f.route_no
          JOIN bookings.seats s ON s.airplane_code = r.airplane_code
        WHERE f.status in ('Departed','Arrived')
        GROUP BY f.flight_id
    ) t
) tt;

WITH empty_flights AS
(
  SELECT f.flight_id, count( bp.flight_id )
  FROM bookings.flights f
    LEFT JOIN bookings.boarding_passes bp ON bp.flight_id = f.flight_id
  WHERE f.status IN ( 'Departed', 'Arrived' )
  GROUP BY f.flight_id
  HAVING count( bp.flight_id ) = 0
)
SELECT count(*) empty,
  CASE WHEN count(*) > 0 THEN 'WARNING: empty cabin' ELSE 'Ok' END verdict
FROM empty_flights;

SELECT a.airplane_code, a.model->>'en' AS model,
  count(DISTINCT r.route_no) AS no_flights,
  CASE
    WHEN count(DISTINCT r.route_no) > 0 AND a.in_use THEN 'Ok'
    WHEN count(DISTINCT r.route_no) = 0 AND a.in_use THEN 'NOT USED'
    WHEN count(DISTINCT r.route_no) > 0 AND NOT a.in_use THEN 'WRONGLY USED'
    WHEN count(DISTINCT r.route_no) = 0 AND NOT a.in_use THEN 'Ok (not in use)'
  END AS verdict
FROM bookings.routes r
  RIGHT JOIN gen.airplanes_data a ON a.airplane_code = r.airplane_code
GROUP BY a.airplane_code, a.model, a.in_use
ORDER BY a.airplane_code;

\echo === Roundtrips to overall tickets
\echo
-------------------------------------------------------------------------------

SELECT
  2 * (count(*) FILTER (WHERE NOT outbound)) / count(*)::numeric AS roundtrip_frac,
  current_setting('gen.roundtrip_frac') AS target_frac
FROM bookings.tickets;

\echo === Passengers per booking
\echo
-------------------------------------------------------------------------------

SELECT count(*) excess_pass_in_booking,
  CASE WHEN count(*) > 0 THEN 'ERROR: max_pass_per_bookings not satisfied' ELSE 'Ok' END verdict
FROM (
  SELECT b.book_ref, count(DISTINCT t.passenger_id) npass
  FROM bookings.bookings b
    JOIN bookings.tickets t ON t.book_ref = b.book_ref
  GROUP BY b.book_ref
  HAVING count(DISTINCT t.passenger_id) > current_setting('gen.max_pass_per_booking')::integer
) t;

SELECT npass, count(*) cnt
FROM (
  SELECT b.book_ref, count(DISTINCT t.passenger_id) npass
  FROM bookings.bookings b
    JOIN bookings.tickets t ON t.book_ref = b.book_ref
  GROUP BY b.book_ref
) t
GROUP BY npass
ORDER BY npass;

\echo === Frequent flyers
\echo
-------------------------------------------------------------------------------

SELECT nbook, count(*) cnt_pass
FROM (
  SELECT passenger_id, count(*) nbook
  FROM (
    SELECT passenger_id
    FROM bookings.tickets
    GROUP BY book_ref, passenger_id
  ) t
  GROUP BY passenger_id
) tt
GROUP BY nbook
ORDER BY nbook;

\echo === Segments per ticket
\echo
-------------------------------------------------------------------------------

WITH segment_counts AS (
  SELECT t.ticket_no, count(*) AS segments
  FROM bookings.tickets t
    JOIN bookings.segments s ON s.ticket_no = t.ticket_no
  GROUP BY t.ticket_no
)
SELECT segments, count(*) cnt
FROM segment_counts
GROUP BY segments
ORDER BY segments;

\echo === Flight statuses
\echo
-------------------------------------------------------------------------------

WITH statuses(status) AS (
  VALUES ('Scheduled'), ('On Time'), ('Delayed'), ('Boarding'), ('Departed'), ('Arrived'), ('Cancelled')
)
SELECT s.status, count(f.flight_id)
FROM statuses s
  LEFT JOIN bookings.flights f ON s.status = f.status
GROUP BY s.status
ORDER BY count(f.flight_id);

\echo === Flight durations
\echo
-------------------------------------------------------------------------------

SELECT count(*) route_flight_mismatch,
  CASE WHEN count(*) > 0 THEN 'ERROR: route and flight discrepancy' ELSE 'Ok' END verdict
FROM bookings.flights f
  JOIN bookings.routes r ON r.route_no = f.route_no AND r.validity @> f.scheduled_departure
WHERE r.duration != f.scheduled_arrival - f.scheduled_departure;

SELECT
  min(sch_duration) min_sch_duration,
  avg(sch_duration) avg_sch_duration,
  max(sch_duration) max_sch_duration,
  min(act_duration) min_act_duration,
  avg(act_duration) avg_act_duration,
  max(act_duration) max_act_duration
FROM (
  SELECT
    scheduled_arrival - scheduled_departure AS sch_duration,
    actual_arrival - actual_departure AS act_duration,
    actual_departure - scheduled_departure AS departure_delay,
    actual_arrival - scheduled_arrival AS arrival_delay
  FROM bookings.flights
) flight \gx

\echo === Flight delays
\echo
-------------------------------------------------------------------------------

SELECT
  min(departure_delay) min_dep_delay,
  avg(departure_delay) avg_dep_delay,
  max(departure_delay) max_dep_delay,
  min(arrival_delay) min_arr_delay,
  avg(arrival_delay) avg_arr_delay,
  max(arrival_delay) max_arr_delay
FROM (
  SELECT
    actual_departure - scheduled_departure AS departure_delay,
    actual_arrival - scheduled_arrival AS arrival_delay
  FROM bookings.flights
) flight \gx

\echo === Overbookings
\echo
-------------------------------------------------------------------------------

WITH seats_available AS
( SELECT airplane_code, fare_conditions, count( * ) AS seats_cnt
  FROM bookings.seats
  GROUP BY airplane_code, fare_conditions
), seats_booked AS
( SELECT flight_id, fare_conditions, count( * ) AS seats_cnt
  FROM bookings.segments
  GROUP BY flight_id, fare_conditions
), overbook AS (
  SELECT f.flight_id, r.route_no, r.airplane_code, sb.fare_conditions,
    sb.seats_cnt AS seats_booked,
    sa.seats_cnt AS seats_available
  FROM bookings.flights AS f
    JOIN bookings.routes AS r ON r.route_no = f.route_no AND r.validity @> f.scheduled_departure
    JOIN seats_booked AS sb ON sb.flight_id = f.flight_id
    JOIN seats_available AS sa ON sa.airplane_code = r.airplane_code
                              AND sa.fare_conditions = sb.fare_conditions
  WHERE sb.seats_cnt > sa.seats_cnt
)
SELECT count(*) overbookings,
  CASE WHEN count(*) > 0 THEN 'ERROR: overbooking' ELSE 'Ok' END verdict
FROM overbook;

\echo === Cancelled flights fraction
\echo
-------------------------------------------------------------------------------

SELECT count(*) FILTER (WHERE status = 'Cancelled') / count(*)::numeric AS actual_cancelled_frac,
  current_setting('gen.cancel_frac') AS target_cancelled_frac
FROM bookings.flights;

\echo === Adjacency of segments
\echo
-------------------------------------------------------------------------------

WITH adjacent_segments AS (
  SELECT s.ticket_no, s.flight_id, r.departure_airport, r.arrival_airport,
    r.arrival_airport != lead( r.departure_airport ) OVER win AS segment_mismatch
  FROM bookings.segments AS s
    JOIN bookings.flights AS f ON f.flight_id = s.flight_id
    JOIN bookings.routes AS r ON r.route_no = f.route_no AND r.validity @> f.scheduled_departure
  WINDOW win AS ( PARTITION BY s.ticket_no ORDER BY f.scheduled_departure )
)
SELECT count(*),
  CASE WHEN count(*) > 0 THEN 'ERROR: non-adjacent segments' ELSE 'Ok' END verdict
FROM adjacent_segments
WHERE segment_mismatch;

\echo === Routes validity ranges
\echo
-------------------------------------------------------------------------------

SELECT CASE
  WHEN range_agg(DISTINCT validity) = tstzmultirange(tstzrange( min(lower(validity)), max(upper(validity)) ))
    THEN 'Ok'
  ELSE 'ERROR: validity ranges have holes'
END verdict
FROM routes;

\echo === Flights consistency with routes
\echo
-------------------------------------------------------------------------------

WITH validities AS (
  SELECT validity, lower( validity ) AS validity_begin, upper( validity ) AS validity_end
  FROM ( SELECT DISTINCT validity FROM bookings.routes ) AS validities
), dates AS (
  SELECT validity, tz, dt::date, extract( isodow FROM dt ) AS dow, validity_begin, validity_end
  FROM validities v
    CROSS JOIN ( SELECT DISTINCT timezone FROM bookings.airports ) tz(tz) 
    CROSS JOIN generate_series(
        timezone( tz, v.validity_begin )::date::timestamp,
        timezone( tz, v.validity_end )::date::timestamp,
        '1 day'::interval ) dt
), schedules AS (
  SELECT r.route_no, a.timezone AS tz, r.validity, r.scheduled_time, unnest( r.days_of_week ) AS dow
  FROM bookings.routes r
    JOIN bookings.airports a ON a.airport_code = r.departure_airport
), dates_to_be AS (
  SELECT s.route_no, s.tz, s.validity, s.dow, timezone( s.tz, d.dt + s.scheduled_time ) AS scheduled_dep
  FROM schedules s
    JOIN dates d ON d.validity = s.validity AND d.tz = s.tz AND d.dow = s.dow
  WHERE timezone( s.tz, d.dt + s.scheduled_time ) >= d.validity_begin
    AND timezone( s.tz, d.dt + s.scheduled_time ) < d.validity_end
), absent AS (
  SELECT dtb.route_no AS route_no, dtb.tz, dtb.validity, dtb.scheduled_dep AS sched_dep_to_be
  FROM bookings.flights f
    JOIN bookings.routes r ON r.route_no = f.route_no AND r.validity @> f.scheduled_departure
    JOIN bookings.airports a ON a.airport_code = r.departure_airport
    RIGHT JOIN dates_to_be dtb ON dtb.route_no = f.route_no AND dtb.validity = r.validity AND dtb.scheduled_dep = f.scheduled_departure
  WHERE f.route_no IS NULL
    AND dtb.scheduled_dep < (SELECT max(scheduled_departure) FROM flights)
), excess AS (
  SELECT f.route_no, a.timezone, r.validity, f.scheduled_departure AS sched_dep_not_to_be
  FROM bookings.flights f
    JOIN bookings.routes r ON r.route_no = f.route_no AND r.validity @> f.scheduled_departure
    JOIN bookings.airports a ON a.airport_code = r.departure_airport
    LEFT JOIN dates_to_be dtb ON dtb.route_no = f.route_no AND dtb.validity = r.validity AND dtb.scheduled_dep = f.scheduled_departure
  WHERE dtb.route_no IS NULL
)
SELECT
  (SELECT count(*) FROM absent) absent_flights,
  (SELECT count(*) FROM excess) excess_flights,
  CASE
    WHEN (SELECT count(*) FROM absent) > 0 AND (SELECT count(*) FROM excess) > 0 THEN 'ERROR: absent and excess flights'
    WHEN (SELECT count(*) FROM absent) > 0 THEN 'ERROR: absent flights'
    WHEN (SELECT count(*) FROM excess) > 0 THEN 'ERROR: excess flights'
    ELSE 'Ok'
  END verdict;

\echo === Timings
\echo
-------------------------------------------------------------------------------

WITH timings AS (
  SELECT
    count(*) FILTER (WHERE f.scheduled_arrival < f.scheduled_departure) AS scheduled_err,
    count(*) FILTER (WHERE f.actual_arrival < f.actual_departure) AS actual_err
  FROM bookings.flights f
)
SELECT scheduled_err, actual_err,
  CASE WHEN scheduled_err + actual_err > 0 THEN 'ERROR: flights timing discrepancy' ELSE 'Ok' END verdict
FROM timings;

WITH boarding_times AS (
  SELECT flight_id, max(boarding_time) AS max_boarding_time
  FROM bookings.boarding_passes
  GROUP BY flight_id
)
SELECT count(*) AS boarding_after_takeoff,
  CASE WHEN count(*) > 0 THEN 'ERROR: boarding after takeoff' ELSE 'Ok' END verdict
FROM bookings.flights f
  JOIN boarding_times AS bt ON bt.flight_id = f.flight_id
WHERE bt.max_boarding_time > f.actual_departure;

SELECT count(*) booking_after_boarding,
  CASE WHEN count(*) > 0 THEN 'ERROR: booking after boarding' ELSE 'Ok' END verdict
FROM bookings.segments s
  JOIN bookings.tickets AS t ON t.ticket_no = s.ticket_no
  JOIN bookings.bookings b ON b.book_ref = t.book_ref
  JOIN bookings.boarding_passes bp ON bp.ticket_no = s.ticket_no AND bp.flight_id = s.flight_id
WHERE b.book_date > bp.boarding_time;

\echo === Miss the flight
\echo
-------------------------------------------------------------------------------

WITH all_segments AS (
  SELECT s.ticket_no, lead( f.actual_departure, 1 ) OVER win - f.actual_arrival AS actual_delta
  FROM bookings.segments s
    JOIN bookings.flights f ON f.flight_id = s.flight_id
  WHERE f.status in ('Departed','Arrived')
  WINDOW win AS ( PARTITION BY s.ticket_no ORDER BY f.scheduled_departure )
), missed AS (
  SELECT ticket_no
  FROM all_segments
  WHERE actual_delta < MISS_FLIGHT_INTERVAL()
  GROUP BY ticket_no
), registered_missed AS (
  SELECT ticket_no
  FROM gen.missed_flights
)
SELECT
  count(*) FILTER (WHERE m.ticket_no = rm.ticket_no) missed_flight_tickets,
  count(*) FILTER (WHERE m.ticket_no IS NULL AND rm.ticket_no IS NOT NULL) incorrectly_registered_misses,
  count(*) FILTER (WHERE m.ticket_no IS NOT NULL AND rm.ticket_no IS NULL) not_registered_misses,
  CASE WHEN count(*) FILTER (WHERE m.ticket_no IS NULL OR rm.ticket_no IS NULL) > 0  THEN 'ERROR: incorrect missed flights' ELSE 'Ok' END verdict
FROM missed m
  FULL JOIN registered_missed rm ON m.ticket_no = rm.ticket_no
;

WITH t AS (
  SELECT bp.ticket_no,
         bp.boarding_time IS NULL AND lead(bp.boarding_time) OVER win IS NOT NULL AS disorder
  FROM bookings.flights f
    JOIN bookings.boarding_passes bp ON bp.flight_id = f.flight_id
  WINDOW win AS ( PARTITION BY bp.ticket_no ORDER BY f.scheduled_departure )
)
SELECT count(DISTINCT ticket_no) boarding_after_miss,
  CASE WHEN count(DISTINCT ticket_no) > 0 THEN 'ERROR: boarding after miss' ELSE 'Ok' END verdict
FROM t
WHERE disorder;

\echo === Interlaced flights
\echo
-------------------------------------------------------------------------------

WITH pass_brefs AS (
  SELECT t.passenger_id, t.book_ref,
    (t.book_ref != lead(t.book_ref) OVER (PARTITION BY t.passenger_id ORDER BY f.scheduled_departure))::integer switch
  FROM bookings.tickets t
    JOIN bookings.segments s ON s.ticket_no = t.ticket_no
    JOIN bookings.flights f ON f.flight_id = s.flight_id
), interlaces AS (
  SELECT passenger_id,
    count(DISTINCT book_ref) bookings,
    sum(switch) switches
  FROM pass_brefs
  GROUP BY passenger_id
  HAVING count(DISTINCT book_ref) <= sum(switch)
)
SELECT count(*) interlaces,
  CASE WHEN count(*) > 0 THEN 'ERROR: interlaced flights' ELSE 'Ok' END verdict
FROM interlaces;

