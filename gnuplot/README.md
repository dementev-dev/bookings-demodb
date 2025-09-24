# Routes Graph for Demo DB

Usage:

```sh
./genplot.sh
```
or
```sh
./genplot.sh N
```

_N_ is the number of routes validity period, from 0 (default) to whatever you have in your Demo DB:

```sql
SELECT row_number() OVER (ORDER BY validity) - 1 n, validity
FROM (SELECT DISTINCT validity FROM routes) r
ORDER BY 1;
```

Writes image to `routes.png`.

Requires psql connection to `demo` database and gnuplot.

