/*
Demonstration Database Generator

Copyright (c) 2025 Postgres Professional

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

--
-- Random numbers generation
--

/*
  Uniform distribution of timestamps.
*/
CREATE OR REPLACE FUNCTION rnd_uniform(
    ts_from timestamptz,
    ts_to timestamptz,
    round_to integer DEFAULT 5 -- round off to minutes
)
RETURNS timestamptz
AS $$
    WITH unrounded as (
      SELECT ts_from + (ts_to - ts_from) * random() as d
    )
    SELECT make_timestamptz(
             extract(year from d)::integer,
             extract(month from d)::integer,
             extract(day from d)::integer,
             extract(hour from d)::integer,
             (trunc(extract(minute from d) / round_to))::integer * round_to,
             0.0,
             to_char(extract(timezone from d)/3600,'SG00')
           )
    FROM unrounded;
$$ LANGUAGE sql;

/*
  Uniform distribution of times.
*/
CREATE OR REPLACE FUNCTION rnd_uniform(
    t_from time,
    t_to time,
    round_to integer DEFAULT 5 -- round off to minutes
)
RETURNS time
AS $$
    WITH unrounded as (
      SELECT t_from + (t_to - t_from) * random() as d
    )
    SELECT make_time(
             extract(hour from d)::integer,
             (trunc(extract(minute from d) / round_to))::integer * round_to,
             0.0
           )
    FROM unrounded;
$$ LANGUAGE sql;

/*
  Uniform distribution of integer random numbers.
*/
CREATE OR REPLACE FUNCTION rnd_uniform(a integer, b integer)
RETURNS integer
AS $$
    SELECT floor(random() * (b-a+1) + a)::integer
$$ LANGUAGE sql;

/*
  Binomial distribution.
*/
CREATE OR REPLACE FUNCTION rnd_binomial(
    n integer,
    p float
)
RETURNS integer
AS $$
DECLARE
    res integer;
    n0 integer;
BEGIN
    res := 0;
    WHILE (n > 0) LOOP
        n0 := least(n, 30);
        res := res + rnd_binomial0(n0,p); -- preventing underflow
        n := n - n0;
    END LOOP;
    RETURN res;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION rnd_binomial0(
    n integer,
    p float
)
RETURNS integer
AS $$
DECLARE
    r float := (1 - p)^n;
    s float := r;
    k integer := 0;
    c float := p / (1 - p);
    a float := random();
BEGIN
    WHILE a > s LOOP
        k := k + 1;
        r := r * c * (n - k + 1) / k;
        s := s + r;
    END LOOP;
    RETURN k;
END;
$$ LANGUAGE plpgsql;

/*
  Exponential distribution (lambda = 1).
  
  For Poisson process:
      E := rnd_exponential
      T = T + E / rate
*/
CREATE OR REPLACE FUNCTION rnd_exponential(
    lambda float
)
RETURNS float
AS $$
    SELECT -(1.0/lambda)*ln(1 - random());
$$ LANGUAGE sql;

/*
  Erlang distribution.
*/
CREATE OR REPLACE FUNCTION rnd_erlang(
    n integer,
    p float
)
RETURNS float
LANGUAGE sql
BEGIN ATOMIC
    SELECT sum(rnd_exponential(1/p)) FROM generate_series(1,n);
END;

/*
  Standard normal distribution (mean = 0, std deviation = 1).
  Can replace with stock random_normal() in PG16.
*/
CREATE OR REPLACE FUNCTION rnd_normal()
RETURNS float
AS $$
DECLARE
    b1 float;
    b2 float;
    d  float;
BEGIN
    LOOP
        b1 := 2.0 * random() - 1.0;
        b2 := 2.0 * random() - 1.0;
        d := b1^2 + b2^2;
        EXIT WHEN d <= 1.0;
    END LOOP;
    RETURN b1 * sqrt( -2.0 * ln(d) / d );
    -- b2 can be used as another independent random variable,
    -- although we do not use this opportunity
END;
$$ LANGUAGE plpgsql;

