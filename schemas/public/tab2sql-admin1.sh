#!/bin/sh

# This script downloads the data for mapping from region codes to region names
# for all the countries around the world.  (Think state or province for
# "region".)  It then converts the tab delimited data into SQL for what we
# want, and runs the SQL to load either a local database or our azure cloud
# database.  (The last step is commented out for safety: we don't want to
# rerun it by accident!)

wget http://download.geonames.org/export/dump/admin1CodesASCII.txt

# 1. CountryCode.AdminCode
# 2. Name
# 3. ASCII Name
# 4. geonames ID

# extract the fields we want
cut -s -f 1,2 admin1CodesASCII.txt >admin1Codes.aaa

# convert tabs to single quotes and commas, first doubling existing single quotes
sed -E "s/'/''/g" admin1Codes.aaa | sed -E "s/\t/','/g" >admin1Codes.bbb

# add the SQL verbiage to each line
sed -E "s/^/INSERT INTO public.geography_regioncodes (code,name) VALUES ('/" admin1Codes.bbb >admin1Codes.ccc
sed -E "s/$/');/" admin1Codes.ccc >admin1Codes.ddd

# create the table
cat <<EOF >table.sss
SET CLIENT_ENCODING TO UTF8;
SET STANDARD_CONFORMING_STRINGS TO ON;
DROP TABLE public.geography_regioncodes;
BEGIN;
CREATE TABLE public.geography_regioncodes (code varchar(25) PRIMARY KEY,
name varchar(200));
END;
EOF
echo "BEGIN;" >begin.x
echo "END;" >end.x
cat table.sss begin.x admin1Codes.ddd end.x >makeAndFillAdmin1.sql

# The details of the database connection depend on where the table is to be.

#echo psql -d TestingGeo -h localhost -U postgres -f makeAndFillAdmin1.sql
#psql -d TestingGeo -h localhost -U postgres -f makeAndFillAdmin1.sql

#echo psql --host=bloom-test-postgres-analytics.postgres.database.azure.com --port=5432 --username=silpgadmin@bloom-test-postgres-analytics --dbname=bloomsegment -f makeAndFillAdmin1.sql
#psql --host=bloom-test-postgres-analytics.postgres.database.azure.com --port=5432 --username=silpgadmin@bloom-test-postgres-analytics --dbname=bloomsegment -f makeAndFillAdmin1.sql
