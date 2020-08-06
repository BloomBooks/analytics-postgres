#!/bin/sh

# This script downloads the data for all cities and towns around the world with
# a population of 500 or more, converts the tab delimited data into SQL for
# what we want, and runs the SQL to load either a local database or our azure
# cloud database.  (The last step is commented out for safety: we don't want
# to rerun it by accident!)

# The zip file is about 9.9 MB, the extracted tab delimited txt file is about 30.3MB
wget http://download.geonames.org/export/dump/cities500.zip
unzip cities500.zip

# data columns per http://download.geonames.org/export/dump/readme.txt
#  1. geonameid         : integer id of record in geonames database
#  2. name              : name of geographical point (utf8) varchar(200)
#  3. asciiname         : name of geographical point in plain ascii characters, varchar(200)
#  4. alternatenames    : alternatenames, comma separated, ascii names automatically transliterated, convenience attribute from alternatename table, varchar(10000)
#  5. latitude          : latitude in decimal degrees (wgs84)
#  6. longitude         : longitude in decimal degrees (wgs84)
#  7. feature class     : see http://www.geonames.org/export/codes.html, char(1)
#  8. feature code      : see http://www.geonames.org/export/codes.html, varchar(10)
#  9. country code      : ISO-3166 2-letter country code, 2 characters
# 10. cc2               : alternate country codes, comma separated, ISO-3166 2-letter country code, 200 characters
# 11. admin1 code       : fipscode (subject to change to iso code), see exceptions below, see file admin1Codes.txt for display names of this code; varchar(20)
# 12. admin2 code       : code for the second administrative division, a county in the US, see file admin2Codes.txt; varchar(80) 
# 13. admin3 code       : code for third level administrative division, varchar(20)
# 14. admin4 code       : code for fourth level administrative division, varchar(20)
# 15. population        : bigint (8 byte int) 
# 16. elevation         : in meters, integer
# 17. dem               : digital elevation model, srtm3 or gtopo30, average elevation of 3''x3'' (ca 90mx90m) or 30''x30'' (ca 900mx900m) area in meters, integer. srtm processed by cgiar/ciat.
# 18. timezone          : the iana timezone id (see file timeZone.txt) varchar(40)
# 19. modification date : date of last modification in yyyy-MM-dd format

# extract the fields we want
cut -s -f 1,2,5,6,9 cities500.txt >cities500.aaa

# extract the country code a second time and the admin1 code, and fuse them for a region
# and join the files together.  (Note that admin1 may be empty: this will be fixed later.)
cut -s -f 1,9,11 cities500.txt | sed -E 's/^([0-9]+)\t([A-Z]+)\t(.*)$/\1\t\2.\3/' >cities500.reg
join -t'	' cities500.aaa cities500.reg >cities500.bbb

# extract the latitude and longitude a second time, then convert to computing
# the postgis geometry value and join the files together
cut -s -f 1,3,4 cities500.aaa | sed -E 's/^([0-9]+)\t([-0-9.]+)\t([-0-9.]+)$/\1\tST_POINT(\3,\2));/' >cities500.pnt
join -t'	' cities500.bbb cities500.pnt >cities500.ccc

# convert tabs to single quotes and commas, first doubling existing single quotes
sed -E "s/'/''/g" cities500.ccc >cities500.ddd
sed -E "s/\t/','/g" cities500.ddd > cities500.eee

# remove unwanted single quotes
sed -E "s/,'ST_POINT/,ST_POINT/" cities500.eee >cities500.fff
sed -E "s/^([0-9]+)',/\1,/" cities500.fff >cities500.ggg

# add the SQL verbiage to each line
sed -E 's/^/INSERT INTO public.geography_city_centers (geoid,closest_city_center,latitude,longitude,countrycode,regioncode,geom) VALUES (/' cities500.ggg >cities500.hhh

# create the table
cat <<EOF >table.sss
SET CLIENT_ENCODING TO UTF8;
SET STANDARD_CONFORMING_STRINGS TO ON;
DROP TABLE public.geography_city_centers;
BEGIN;
CREATE TABLE public.geography_city_centers (geoid bigint PRIMARY KEY,
closest_city_center varchar(200),
latitude numeric,
longitude numeric,
countrycode char(3),
regioncode varchar(25));
SELECT AddGeometryColumn('','geography_city_centers','geom','0','POINT',2);
END;
EOF
echo "BEGIN;" >begin.x
echo "UPDATE public.geography_city_centers SET regioncode=NULL WHERE regioncode LIKE '__.';" >end.x
echo "END;" >>end.x
cat table.sss begin.x cities500.hhh end.x >makeAndFillCities500.sql

# The details of the database connection depend on where the table is to be.

# This is for my local testing setup.
#echo psql -d TestingGeo -h localhost -U postgres -f makeAndFillCities500.sql
#psql -d TestingGeo -h localhost -U postgres -f makeAndFillCities500.sql

# This is for creating and filling the table in the azure database in the cloud.
# This command took about 5-6 hours to complete:  200,000 rows to insert, probably around 10 rows per second.
# It might or might not go faster if we bundled the rows together 10 at a time or so, or if we split the BEGIN...END
# block from the entire set of rows to 1000 rows at a time.
#echo psql --host=bloom-test-postgres-analytics.postgres.database.azure.com --port=5432 --username=silpgadmin@bloom-test-postgres-analytics --dbname=bloomsegment -f makeAndFillCities500.sql
#psql --host=bloom-test-postgres-analytics.postgres.database.azure.com --port=5432 --username=silpgadmin@bloom-test-postgres-analytics --dbname=bloomsegment -f makeAndFillCities500.sql
