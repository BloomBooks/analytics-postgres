-- Search the geography_city_centers table to find the city whose center is closest to the
-- incoming latitude and longitude.  The bound parameter limits how far away (in degrees)
-- to search for city locations.
CREATE OR REPLACE FUNCTION public.NearestTownOf500(myLatitude numeric, myLongitude numeric, bound numeric)
	RETURNS bigint AS $$
DECLARE
	closest bigint;
	myLocation geometry;
BEGIN
	SELECT ST_POINT(myLongitude, myLatitude) INTO myLocation;
	SELECT geoid
	FROM public.geography_city_centers
	WHERE public.ST_DISTANCE(myLocation, geom) < bound
	ORDER BY public.ST_DISTANCE(myLocation, geom)
	LIMIT 1 INTO closest;
	RETURN closest;
END;
$$ LANGUAGE plpgsql;

-- Implement the trigger function for finding the location when inserting a row into any
-- table with latitude, longitude, city_center_id, context_ip, and location_uid columns.
-- If NEW.latitude and NEW.longitude are set, then NEW.city_center_id is set and the
-- function quits.  If either NEW.latitude or NEW.longitude is NULL, then NEW.context_ip
-- is used to try to guess where the user is, storing the result in NEW.location_uid.
-- Thus, either NEW.city_center_id or NEW.location_uid is set by this function, but not
-- both.
CREATE OR REPLACE FUNCTION public.find_closest_city_or_location_uid_fctn()
	RETURNS trigger AS $$
DECLARE ip_address_temp text;
DECLARE hold_region varchar(100);
DECLARE hold_city varchar(100);
DECLARE country_name_temp varchar(64);
DECLARE country_code_temp char(3);
DECLARE loc_uid_temp bigint;
DECLARE counter bigint;
DECLARE geoid bigint;
BEGIN
	IF NEW.latitude IS NOT NULL AND NEW.longitude IS NOT NULL THEN
		-- search for towns within 1 degree (~69 miles/111 km near the equator)
		SELECT public.NearestTownOf500(NEW.latitude,NEW.longitude,1.0) INTO geoid;
		IF geoid IS NULL THEN
			-- if nothing found, search twice as far for any towns
			SELECT public.NearestTownOf500(NEW.latitude,NEW.longitude,2.0) INTO geoid;
		END IF;
		IF geoid IS NOT NULL THEN
			NEW.city_center_id := geoid;
			RETURN NEW;
		END IF;
	END IF;
	--
	-- The rest of this method is essentially unchanged from before, using the ip address
	-- to get the location.
	--
	counter := NULL;
	SELECT MAX(c.loc_uid) FROM public.countryregioncitylu AS c INTO counter;
	ip_address_temp := NEW.context_ip;
	country_code_temp :=NULL;
	country_name_temp :=NULL;
	hold_region :=NULL;
	hold_city :=NULL;
	SELECT b.country_code, b.country_name, b.region, b.city FROM public.ipv42location AS b
		WHERE (SELECT public.ip2ipv4(ip_address_temp))
		BETWEEN b.ipv4_from and b.ipv4_to
		INTO country_code_temp, country_name_temp, hold_region, hold_city ;
	IF country_code_temp ='-' or public.empty_to_null(country_code_temp) IS NULL THEN
		country_code_temp := '-';
	END IF;
	IF country_name_temp ='-' or public.empty_to_null(country_name_temp) IS NULL THEN
		country_name_temp := '-';
	END IF;
	IF hold_region ='-' or public.empty_to_null(hold_region) IS NULL THEN
		hold_region := '-';
	END IF;
	IF hold_city ='-' or public.empty_to_null(hold_city) IS NULL THEN
		hold_city := '-';
	END IF;
	loc_uid_temp := NULL;
	SELECT i.loc_uid from public.countryregioncitylu AS i
		WHERE i.country_code = country_code_temp
		AND i.region = hold_region
		AND i.city = hold_city
		INTO loc_uid_temp;
	IF loc_uid_temp IS NULL THEN
		counter := counter + CAST('1' AS bigint);
		loc_uid_temp := counter ;
		INSERT INTO  public.countryregioncitylu as p
		( loc_uid, country_code, country_name, region, city  )
		VALUES
		( loc_uid_temp, country_code_temp, country_name_temp, hold_region, hold_city )
		ON CONFLICT ON CONSTRAINT countryregioncitylu_db1_pkey DO NOTHING;
	END IF;
	--
	NEW.location_uid := loc_uid_temp;
	--
RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create a trigger dealing with finding the location when adding a row to the
-- bloomreadertest.book_or_shelf_opened table.
DROP TRIGGER insert_location_uid ON bloomreadertest.book_or_shelf_opened;
CREATE TRIGGER insert_location_uid
	BEFORE INSERT
	ON bloomreadertest.book_or_shelf_opened
	FOR EACH ROW
	EXECUTE PROCEDURE public.find_closest_city_or_location_uid_fctn();

-- Create a trigger dealing with finding the location when adding a row to the
-- bloomreadertest.pages_read table.
DROP TRIGGER insert_location_uid ON bloomreadertest.pages_read;
CREATE TRIGGER insert_location_uid
	BEFORE INSERT
	ON bloomreadertest.pages_read
	FOR EACH ROW
	EXECUTE PROCEDURE public.find_closest_city_or_location_uid_fctn();

-- Create a trigger dealing with finding the location when adding a row to the
-- bloomreaderbeta.book_or_shelf_opened table.
DROP TRIGGER insert_location_uid ON bloomreaderbeta.book_or_shelf_opened;
CREATE TRIGGER insert_location_uid
	BEFORE INSERT
	ON bloomreaderbeta.book_or_shelf_opened
	FOR EACH ROW
	EXECUTE PROCEDURE public.find_closest_city_or_location_uid_fctn();

-- Create a trigger dealing with finding the location when adding a row to the
-- bloomreaderbeta.pages_read table.
DROP TRIGGER insert_location_uid ON bloomreaderbeta.pages_read;
CREATE TRIGGER insert_location_uid
	BEFORE INSERT
	ON bloomreaderbeta.pages_read
	FOR EACH ROW
	EXECUTE PROCEDURE public.find_closest_city_or_location_uid_fctn();

-- Create a trigger dealing with finding the location when adding a row to the
-- bloomreader.book_or_shelf_opened table.
DROP TRIGGER insert_location_uid ON bloomreader.book_or_shelf_opened;
CREATE TRIGGER insert_location_uid
	BEFORE INSERT
	ON bloomreader.book_or_shelf_opened
	FOR EACH ROW
	EXECUTE PROCEDURE public.find_closest_city_or_location_uid_fctn();

-- Create a trigger dealing with finding the location when adding a row to the
-- bloomreader.pages_read table.
DROP TRIGGER insert_location_uid ON bloomreader.pages_read;
CREATE TRIGGER insert_location_uid
	BEFORE INSERT
	ON bloomreader.pages_read
	FOR EACH ROW
	EXECUTE PROCEDURE public.find_closest_city_or_location_uid_fctn();
