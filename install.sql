/*
Demonstration Database Generator

Copyright (c) 2025 Postgres Professional

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

DROP DATABASE demo;
CREATE DATABASE demo;
\c demo
CREATE EXTENSION btree_gist;
CREATE EXTENSION earthdistance CASCADE;
CREATE EXTENSION dblink;
-- CREATE EXTENSION unaccent; -- to convert names to machine-readable format, see get_passenger_name()

-- speed up generation
ALTER DATABASE demo SET synchronous_commit = off;
-- use UTC to avoid daylight-saving problems
ALTER DATABASE demo SET timezone = 'Etc/UTC';

-- connection string for dblink
ALTER DATABASE demo SET gen.connstr = 'dbname=demo';

-- airlines company name
ALTER DATABASE demo SET gen.airlines_name = 'PostgresPro';
-- airlines code for flight numbers
ALTER DATABASE demo SET gen.airlines_code = 'PG';

-- coefficient to convert gen.airports_data.traffic to number of bookings per week
ALTER DATABASE demo SET gen.traffic_coeff = 50.0;
-- fraction of domestic flights
ALTER DATABASE demo SET gen.domestic_frac = 0.9;
-- fraction of roundtrip tickets
ALTER DATABASE demo SET gen.roundtrip_frac = 0.9;
-- fraction of delayed flights
ALTER DATABASE demo SET gen.delay_frac = 0.05;
-- fraction of cancelled flights
ALTER DATABASE demo SET gen.cancel_frac = 0.005;
-- exchange rate: minutes of flight to currency
ALTER DATABASE demo SET gen.exchange = 50.0;
-- max passengers in a booking
ALTER DATABASE demo SET gen.max_pass_per_booking = 5;
-- minimum allowed transfer time, hours
ALTER DATABASE demo SET gen.min_transfer = 2;
-- maximum allowed transfer time, hours
ALTER DATABASE demo SET gen.max_transfer = 48;
-- maximum allowed segments (hops) per ticket
ALTER DATABASE demo SET gen.max_hops = 4;

-- log only messages with the specified of higher severity (0 = highest)
ALTER DATABASE demo SET gen.log_severity = 0;

-- language
ALTER DATABASE demo SET bookings.lang = 'en';
-- search_path
ALTER DATABASE demo SET search_path = bookings,"$user",public;
\c

SET search_path = public; -- to create routines in public by default
\i tables.sql
\i random.sql
\i names.sql
\i engine.sql
\i route.sql
\i booking.sql
CALL calc_names_cume_dist();

SET search_path = bookings,"$user",public; -- to create routines in public by default

