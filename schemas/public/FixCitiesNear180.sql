-- For cities/towns within 2 degrees of 180 degress longitude, add another entry that gives the same value
-- in some sense but goes over 180.  This allows the city to be found for locations just across the 180
-- degree line even for geometry measurements, which give a distance close to 360 otherwise.
BEGIN;
INSERT INTO public.geography_city_centers (geoid,closest_city_center,latitude,longitude,countrycode,regioncode,geom) VALUES (1001,'Levuka','-18.06667','-180.68333','FJ','FJ.02',ST_POINT(-18.06667,-180.68333));
INSERT INTO public.geography_city_centers (geoid,closest_city_center,latitude,longitude,countrycode,regioncode,geom) VALUES (1002,'Labasa','-16.4332','-180.63549','FJ','FJ.03',ST_POINT(-16.4332,-180.63549));
INSERT INTO public.geography_city_centers (geoid,closest_city_center,latitude,longitude,countrycode,regioncode,geom) VALUES (1003,'Beringovskiy','63.06101','-180.64954','RU','RU.15',ST_POINT(63.06101,-180.64954));
INSERT INTO public.geography_city_centers (geoid,closest_city_center,latitude,longitude,countrycode,regioncode,geom) VALUES (1004,'Egvekinot','66.32166','180.87802','RU','RU.15',ST_POINT(66.32166,180.87802));
INSERT INTO public.geography_city_centers (geoid,closest_city_center,latitude,longitude,countrycode,regioncode,geom) VALUES (1005,'Funafuti','-8.52425','-180.80583','TV','TV.FUN',ST_POINT(-8.52425,-180.80583));
INSERT INTO public.geography_city_centers (geoid,closest_city_center,latitude,longitude,countrycode,regioncode,geom) VALUES (1006,'Vaiaku Village','-8.52544','-180.8059','TV','TV.FUN',ST_POINT(-8.52544,-180.8059));
INSERT INTO public.geography_city_centers (geoid,closest_city_center,latitude,longitude,countrycode,regioncode,geom) VALUES (1007,'Alapi Village','-8.52074','-180.8032','TV','TV.FUN',ST_POINT(-8.52074,-180.8032));
INSERT INTO public.geography_city_centers (geoid,closest_city_center,latitude,longitude,countrycode,regioncode,geom) VALUES (1008,'Senala Village','-8.5174','-180.80171','TV','TV.FUN',ST_POINT(-8.5174,-180.80171));
INSERT INTO public.geography_city_centers (geoid,closest_city_center,latitude,longitude,countrycode,regioncode,geom) VALUES (1009,'Fakaifou Village','-8.51758','-180.79906','TV','TV.FUN',ST_POINT(-8.51758,-180.79906));
INSERT INTO public.geography_city_centers (geoid,closest_city_center,latitude,longitude,countrycode,regioncode,geom) VALUES (1010,'Teone Village','-8.49922','-180.80498','TV','TV.FUN',ST_POINT(-8.49922,-180.80498));
INSERT INTO public.geography_city_centers (geoid,closest_city_center,latitude,longitude,countrycode,regioncode,geom) VALUES (1011,'Tolaga Bay',-38.36667,-181.7,'NZ','NZ.F1',ST_POINT(-38.36667,-181.7));
INSERT INTO public.geography_city_centers (geoid,closest_city_center,latitude,longitude,countrycode,regioncode,geom) VALUES (1012,'Suva',-18.14161,-181.55851,'FJ','FJ.01',ST_POINT(-18.14161,-181.55851));
INSERT INTO public.geography_city_centers (geoid,closest_city_center,latitude,longitude,countrycode,regioncode,geom) VALUES (1013,'Tubou',-18.23652,181.18768,'FJ','FJ.02',ST_POINT(-18.23652,181.18768));
INSERT INTO public.geography_city_centers (geoid,closest_city_center,latitude,longitude,countrycode,regioncode,geom) VALUES (1014,'Ruatoria',-37.88333,-181.66667,'NZ','NZ.F1',ST_POINT(-37.88333,-181.66667));
INSERT INTO public.geography_city_centers (geoid,closest_city_center,latitude,longitude,countrycode,regioncode,geom) VALUES (1015,'Gisborne',-38.65333,-181.99583,'NZ','NZ.F1',ST_POINT(-38.65333,-181.99583));
INSERT INTO public.geography_city_centers (geoid,closest_city_center,latitude,longitude,countrycode,regioncode,geom) VALUES (1016,'Leningradskiy',69.36012,-181.59897,'RU','RU.15',ST_POINT(69.36012,-181.59897));
INSERT INTO public.geography_city_centers (geoid,closest_city_center,latitude,longitude,countrycode,regioncode,geom) VALUES (1017,'Asau Village',-7.49026,-181.31984,'TV','TV.VAI',ST_POINT(-7.49026,-181.31984));
INSERT INTO public.geography_city_centers (geoid,closest_city_center,latitude,longitude,countrycode,regioncode,geom) VALUES (1018,'Motufoua School',-7.48996,-181.30744,'TV','TV.VAI',ST_POINT(-7.48996,-181.30744));
INSERT INTO public.geography_city_centers (geoid,closest_city_center,latitude,longitude,countrycode,regioncode,geom) VALUES (1019,'Savave Village',-8.02731,-181.68649,'TV','TV.NKF',ST_POINT(-8.02731,-181.68649));
INSERT INTO public.geography_city_centers (geoid,closest_city_center,latitude,longitude,countrycode,regioncode,geom) VALUES (1020,'Alo',-14.31096,181.88906,'WF','WF.98611',ST_POINT(-14.31096,181.88906));
INSERT INTO public.geography_city_centers (geoid,closest_city_center,latitude,longitude,countrycode,regioncode,geom) VALUES (1021,'Leava',-14.29333,181.84167,'WF','WF.98612',ST_POINT(-14.29333,181.84167));
END;
