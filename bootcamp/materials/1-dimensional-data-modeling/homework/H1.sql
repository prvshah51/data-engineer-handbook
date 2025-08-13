
-- Homework 1

select * from actor_films limit 11;

-- 1:1
CREATE TYPE films AS (
                        film TEXT,
                        votes INTEGER,
                        rating REAL,
                        filmid TEXT
                            );
-- 1:2
CREATE TYPE quality_class as ENUM('star', 'good', 'average', 'bad');

CREATE TABLE actors(
    actorid TEXT,
    actor TEXT,
    films films[],
    quality_class quality_class,
    is_active bool,
    current_year INTEGER,
    PRIMARY KEY (actorid)
);

select * from actors;

-- 2:
WITH
    today_start AS (
        SELECT actorid, actor,
               ROW(film, votes, rating, filmid)::films AS films,
               rating,
               year
        FROM actor_films
        WHERE year = 1973
    ),
    today AS (
        SELECT actorid, actor,
               ARRAY_AGG(films) AS films,
               AVG(rating) AS rating,
               year
        FROM today_start
        GROUP BY actorid, actor, year
    ),
    yesterday AS (
        SELECT * FROM actors WHERE current_year = 1972
    )
INSERT INTO actors (actorid, actor, films, quality_class, is_active, current_year)
SELECT
    COALESCE(t.actorid, y.actorid) AS actorid,
    COALESCE(t.actor, y.actor) AS actor,
    CASE
        WHEN y.films IS NULL THEN t.films
        WHEN t.year IS NOT NULL THEN y.films || t.films
        ELSE y.films
    END AS films,
    CASE
        WHEN t.rating IS NOT NULL THEN
            (CASE
                WHEN t.rating > 8 THEN 'star'
                WHEN t.rating > 7 THEN 'good'
                WHEN t.rating > 6 THEN 'average'
                ELSE 'bad'
            END)::quality_class
        ELSE y.quality_class
    END AS quality_class,
    CASE
        WHEN t.year IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_active,
    COALESCE(t.year, y.current_year + 1) AS current_year
FROM today t
FULL OUTER JOIN yesterday y ON t.actorid = y.actorid
ON CONFLICT (actorid) DO UPDATE SET
    films = CASE
        WHEN EXCLUDED.is_active THEN actors.films || EXCLUDED.films
        ELSE actors.films
    END,
    quality_class = EXCLUDED.quality_class,
    is_active = EXCLUDED.is_active,
    current_year = EXCLUDED.current_year,
    actor = EXCLUDED.actor;

-- 3:

create table actors_history_scd(
    actorid TEXT NOT NULL ,
    actor TEXT NOT NULL ,
    quality_class quality_class NOT NULL,
    is_active BOOL NOT NULL ,
    start_year INTEGER NOT NULL ,
    end_year INTEGER,
    PRIMARY KEY (actorid,start_year)
);


-- 4:
WITH first AS (
    SELECT
        actor,
        actorid,
        year,
        AVG(rating) AS avg_rating
    FROM actor_films
    GROUP BY actorid, actor, year
),
classified AS (
    SELECT *,
        CASE
            WHEN avg_rating > 8 THEN 'star'
            WHEN avg_rating > 7 THEN 'good'
            WHEN avg_rating > 6 THEN 'average'
            ELSE 'bad'
        END AS quality_class
    FROM first
),
second AS (
    SELECT actor, actorid, generate_series(MIN(year), MAX(year)) AS year
    FROM first
    GROUP BY actor, actorid
)
SELECT
    s.actor,
    s.actorid,
    s.year,
    COALESCE(c.quality_class,
        (
            SELECT qc.quality_class
            FROM classified qc
            WHERE qc.actorid = s.actorid AND qc.year < s.year
            ORDER BY qc.year DESC
            LIMIT 1
        )
    ) AS quality_class,
    c.quality_class IS NOT NULL AS is_active
FROM second s
LEFT JOIN classified c ON s.actorid = c.actorid AND s.year = c.year
ORDER BY s.actorid, s.year;


-- 5:
CREATE TABLE actors_history_scd (
    actorid TEXT NOT NULL,
    actor TEXT NOT NULL,
    quality_class quality_class NOT NULL,
    is_active BOOLEAN NOT NULL,
    current_year INTEGER NOT NULL
);


-- Incremental query for actors_history_scd (Task 5)
-- Change the year value (1970) in the new_year_data CTE for each new iteration

WITH new_year_data AS (
    SELECT actorid, actor,
           CASE
               WHEN AVG(rating) > 8 THEN 'star'
               WHEN AVG(rating) > 7 THEN 'good'
               WHEN AVG(rating) > 6 THEN 'average'
               ELSE 'bad'
           END::quality_class AS quality_class,
           TRUE AS is_active,  -- Active since there are films this year
           1972 AS current_year  -- Change this year value for each new iteration
    FROM actor_films
    WHERE year = 1972  -- Change this year value for each new iteration
    GROUP BY actorid, actor
),
latest_scd AS (
    SELECT actor, actorid, quality_class, is_active, current_year
    FROM actors_history_scd
    WHERE current_year = (SELECT MAX(current_year) FROM actors_history_scd h2 WHERE h2.actorid = actors_history_scd.actorid)
),
changes AS (
    SELECT n.actorid, n.actor, n.quality_class, n.is_active, n.current_year
    FROM new_year_data n
    LEFT JOIN latest_scd l ON n.actorid = l.actorid
    WHERE l.actorid IS NULL  -- New actor
       OR l.quality_class <> n.quality_class
       OR l.is_active <> n.is_active
    UNION
    -- Include inactive actors from previous SCD state who have no films this year
    SELECT l.actorid, l.actor, l.quality_class, FALSE AS is_active, 1972 AS current_year  -- Change this year value
    FROM latest_scd l
    LEFT JOIN new_year_data n ON l.actorid = n.actorid
    WHERE n.actorid IS NULL
      AND l.is_active = TRUE  -- Was active previously, now inactive
)
INSERT INTO actors_history_scd (actorid, actor, quality_class, is_active, current_year)
SELECT actorid, actor, quality_class, is_active, current_year
FROM changes;