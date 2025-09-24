/*
Demonstration Database Generator

Copyright (c) 2025 Postgres Professional

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

--
-- Passengers' names handling
--

/*
  Returns random passenger (ID and name) for the given country.
  ID is locked till the end of the trip so that it cannot be used
  in other bookings or flights.
*/
CREATE OR REPLACE FUNCTION get_passenger(
    country IN text,
    start_of_trip IN timestamptz,
    end_of_trip IN timestamptz,
    passenger_id OUT text,
    passenger_name OUT text
)
AS $$
DECLARE
    passenger_locked_until timestamptz;
BEGIN
    end_of_trip := end_of_trip + interval '1 week'; -- stay home for a while after the last trip

    -- choose suitable passenger ID
    LOOP
        passenger_id := get_passenger_id(country);

        -- proceeding without lock on passenger ID can cause deadlock while inserting tuple
        -- into passenger_pkey index
        CONTINUE WHEN NOT pg_try_advisory_xact_lock(hashtext(passenger_id));

        -- check if ID is already known and is available
        SELECT p.name, p.locked_until
        INTO passenger_name, passenger_locked_until
        FROM gen.passengers p
        WHERE p.id = passenger_id;

        CONTINUE WHEN passenger_locked_until >= start_of_trip;

        EXIT;
    END LOOP;

    IF passenger_name IS NULL THEN
        -- new passenger; generate name and remember it
        passenger_name := get_passenger_name(country);
        INSERT INTO gen.passengers(id, name, locked_until)
            VALUES (passenger_id, passenger_name, end_of_trip);
    ELSE
        -- existing passenger; update availablity
        UPDATE gen.passengers p
        SET locked_until = end_of_trip
        WHERE p.id = passenger_id;
    END IF;
END;
$$ LANGUAGE plpgsql;

/*
  Random passenger ID for the given country.
  ID is constructed from country code plus 13 random digits.
  We choose from 10 million (7 digits) unique numbers, adding 6 more non-random digits.
*/
CREATE OR REPLACE FUNCTION get_passenger_id(country text) RETURNS text
AS $$
DECLARE
    rnd1 float;
    hash float;
    rnd2 float;
BEGIN
    -- some passengers flight more frequent than others
    LOOP
        rnd1 := round(random() * 1e7);
        hash := abs(hashtext(country || rnd1::text)) / 2147483648.0; -- 0..1
        EXIT WHEN random() < hash;
    END LOOP;
    rnd2 := abs(hashfloat8(rnd1));
    RETURN upper(country) || ' ' ||  -- country code
           lpad(rnd1::text,7,'0') || -- 7 digits
           lpad(rnd2::text,6,'0');   -- 6 digits
END;
$$ LANGUAGE plpgsql;

/*
  Random passenger name for the given country.
*/
CREATE OR REPLACE FUNCTION get_passenger_name(country text) RETURNS text
AS $$
<<local>>
DECLARE
    first_name text;
    last_name text;
    grp text;
    rnd float;
BEGIN
    LOOP
        -- first choose a random first name
        rnd := random();
        SELECT fn.name, fn.grp
          INTO first_name, grp
          FROM gen.firstnames fn
         WHERE fn.country = get_passenger_name.country
           AND fn.cume_dist > rnd
         ORDER BY fn.cume_dist
         LIMIT 1;

        -- then choose a random last name from the same group
        rnd := random();
        SELECT ln.name
          INTO last_name
          FROM gen.lastnames ln
         WHERE ln.country = get_passenger_name.country
           AND ln.cume_dist > rnd
           AND ln.grp = local.grp
         ORDER BY ln.cume_dist
         LIMIT 1;

        EXIT WHEN first_name != last_name; -- prevent names like Douglas Douglas
    END LOOP;

    RETURN first_name || ' ' || last_name;
    -- for machine-readable format: RETURN upper(unaccent(first_name || ' ' || last_name));
END;
$$ LANGUAGE plpgsql;

/*
  Calculate cumulative distribution for first and last names.
  This procedure must be called once before generating names.
*/
CREATE OR REPLACE PROCEDURE calc_names_cume_dist()
AS $$
    UPDATE gen.firstnames dst
    SET cume_dist = src.cume_dist
    FROM (
      SELECT country, name,
             (sum(qty) OVER (PARTITION BY country ORDER BY qty))::numeric /
              sum(qty) OVER (PARTITION BY country) cume_dist
      FROM gen.firstnames
    ) src
    WHERE dst.country = src.country
    AND dst.name = src.name;

    UPDATE gen.lastnames dst
    SET cume_dist = src.cume_dist
    FROM (
      SELECT country, name, grp,
             (sum(qty) OVER (PARTITION BY country,grp ORDER BY qty))::numeric /
              sum(qty) OVER (PARTITION BY country,grp) cume_dist
      FROM gen.lastnames
    ) src
    WHERE dst.country = src.country
    AND dst.name = src.name
    AND dst.grp = src.grp;
$$ LANGUAGE sql;

