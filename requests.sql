-- Data taken from http://download.geonames.org/export/dump/
-- Measure
SELECT COUNT(*), pg_size_pretty(pg_relation_size('cities')) AS data, pg_size_pretty(pg_indexes_size('cities')) AS idxs
FROM cities;



-- Index
CREATE INDEX pizzerias_location_idx ON pizzerias USING gist(ll_to_earth(lat,lng));




-- Get coordinates
SELECT * FROM cities WHERE lower(name) = 'bar' AND region = 'ME';



-- First version, simpler
SELECT *, (point(lng, lat) <@> point(19.1, 42.1)) * 1609.344 AS distance
FROM
	cities
WHERE
    (point(lng, lat) <@> point(19.1, 42.1)) < (10000 / 1609.344)
ORDER BY
	distance;





-- Secont attempt
EXPLAIN SELECT name, (point(c.lng, c.lat) <@> point(bar_me.lng, bar_me.lat)) * 1609.344 AS distance
FROM
	cities c,
	LATERAL (
		SELECT id, lat, lng
		FROM
			cities
		WHERE
			name = 'Bar'
			AND region = 'ME') AS bar_me
WHERE
	c.id <> bar_me.id
	AND (point(c.lng, c.lat) <@> point(bar_me.lng, bar_me.lat)) < (10000/1609.344)
ORDER BY
	distance;




-- Third attempt - cubes
SELECT *, ROUND(earth_distance(ll_to_earth(42.1, 19.1), ll_to_earth(lat, lng))::NUMERIC, 2) AS distance
FROM
	cities
WHERE
	earth_box(ll_to_earth (42.1, 19.1), 10000) @> ll_to_earth (lat, lng)
	AND earth_distance(ll_to_earth (42.1, 19.1), ll_to_earth (lat, lng)) < 10000
ORDER BY
	distance;



-- Compare
SELECT name,
    ST_Distance(ST_MakePoint(lng, lat)::geography, ST_MakePoint(19.1, 42.1)::geography) AS postgis_distance,
    earth_distance(ll_to_earth(42.1, 19.1), ll_to_earth (lat, lng)) AS earth_distance,
    (point(lng, lat) <@> point(19.1, 42.1)) * 1609.344 AS point_distance
FROM
	cities
WHERE
	earth_box(ll_to_earth (42.1, 19.1), 10000) @> ll_to_earth (lat, lng)
	AND earth_distance(ll_to_earth (42.1, 19.1), ll_to_earth (lat, lng)) < 10000
ORDER BY
	earth_distance;




-- Most faravay pizzerias and cities
SELECT name,
    (SELECT name FROM cities_min c
        ORDER BY earth_distance(ll_to_earth(c.lat, c.lng), ll_to_earth(p.lat, p.lng)) LIMIT 1) AS city,
    earth_distance(ll_to_earth(p.lat, p.lng), ll_to_earth(42.1, 19.1)) AS distance
FROM
    pizzerias p
ORDER BY
    distance
    DESC
LIMIT 25;




-- Most faravay pizzerias and cities
WITH pizzerias AS (
    SELECT
        name,
        ll_to_earth(lat, lng) as point,
        earth_distance(ll_to_earth(lat, lng), ll_to_earth(42.1, 19.1)) AS distance
    FROM pizzerias
    ORDER BY
        distance
        DESC
    LIMIT 25
)
SELECT c.name AS city, p.name, p.distance
    FROM pizzerias p,
         LATERAL (SELECT name FROM cities_min c
                  ORDER BY earth_distance(ll_to_earth(c.lat, c.lng), p.point) LIMIT 1) c;




-- Most pizza loving cities
SELECT c.name, count(cp)
    FROM cities_min c,
         LATERAL (SELECT "name"
                  FROM pizzerias p
                  WHERE earth_distance(ll_to_earth(p.lat, p.lng), ll_to_earth(c.lat, c.lng)) < 10000
         ) AS cp
    GROUP BY c.name
    ORDER BY count(cp) DESC;
