cat LICENSE | sed 's/^/-- /'
echo
pg_dump --clean --create --no-owner --no-acl --schema=bookings \
  --extension=btree_gist --extension=cube --extension=earthdistance demo $@ \
  | sed 's/LOCALE_PROVIDER = libc //' \
  | sed 's/UNLOGGED //' \
  | sed '/ALTER DATABASE demo SET synchronous_commit/d' \
  | sed '/ALTER DATABASE demo SET "TimeZone/d' \
  | sed '/ALTER DATABASE demo SET "gen/d'
