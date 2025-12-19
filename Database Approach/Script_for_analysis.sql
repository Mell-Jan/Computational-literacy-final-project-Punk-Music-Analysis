
-- Create Table with english speaking areas, as areas vary between towns, states and countries. 
Create table english_speaking_areas as
Select 'United States' as area 

INSERT INTO english_speaking_areas (area)
SELECT area
FROM (VALUES
  -- Countries / national regions
  ('United States'),
  ('United Kingdom'),
  ('England'),
  ('Scotland'),
  ('Wales'),
  ('Northern Ireland'),
  ('Ireland'),
  ('Australia'),
  ('New Zealand'),
  ('Canada'),
  ('Jamaica'),
  ('South Africa'),
  ('Bermuda'),

  -- US states
  ('Alabama'),
  ('Arizona'),
  ('Arkansas'),
  ('California'),
  ('Colorado'),
  ('Connecticut'),
  ('Florida'),
  ('Hawaii'),
  ('Indiana'),
  ('Kentucky'),
  ('Massachusetts'),
  ('Michigan'),
  ('Mississippi'),
  ('Montana'),
  ('North Carolina'),
  ('Ohio'),
  ('Pennsylvania'),
  ('Texas'),
  ('Utah'),
  ('Virginia'),
  ('Wisconsin'),

  -- UK regions / counties
  ('West Yorkshire'),
  ('Wiltshire'),
  ('Buckinghamshire'),
  ('Cumbria'),
  ('Derbyshire'),
  ('Leicestershire'),
  ('North Ayrshire'),
  ('Tyne and Wear'),

  -- US cities from your list
  ('Austin'),
  ('Baltimore'),
  ('Berkeley'),
  ('Benicia'),
  ('Boston'),
  ('Brooklyn'),
  ('Buffalo'),
  ('Chapel Hill'),
  ('Charlotte'),
  ('Chattanooga'),
  ('Chicago'),
  ('Cincinnati'),
  ('Cleveland'),
  ('Columbus'),
  ('Dallas'),
  ('Delray Beach'),
  ('Denver'),
  ('Detroit'),
  ('Edina'),
  ('Fort Wayne'),
  ('Franklin Park'),
  ('Fullerton'),
  ('Gainesville'),
  ('Haddon Heights'),
  ('Hermosa Beach'),
  ('Hoboken'),
  ('Hollywood'),
  ('Honolulu'),
  ('Houston'),
  ('Huntington'),
  ('Huntington Beach'),
  ('Indianapolis'),
  ('Kansas City'),
  ('Lafayette'),
  ('Lansing'),
  ('Long Beach'),
  ('Long Island'),
  ('Los Angeles'),
  ('Louisville'),
  ('Madison'),
  ('Manhattan'),
  ('Memphis'),
  ('Miami'),
  ('Milwaukee'),
  ('Minneapolis'),
  ('Mt. Juliet'),
  ('Nashville'),
  ('New Haven'),
  ('New Orleans'),
  ('New York'),
  ('Oakland'),
  ('Olympia'),
  ('Orange County'),
  ('Orlando'),
  ('Oxnard'),
  ('Petaluma'),
  ('Phoenix'),
  ('Philadelphia'),
  ('Pittsburgh'),
  ('Plainfield'),
  ('Portland'),
  ('Providence'),
  ('Raleigh'),
  ('Riverside'),
  ('Rochester'),
  ('Roanoke'),
  ('Salt Lake City'),
  ('San Diego'),
  ('San Francisco'),
  ('Santa Barbara'),
  ('Santa Cruz'),
  ('San Pedro'),
  ('Sarasota'),
  ('Savannah'),
  ('Seattle'),
  ('Sherman'),
  ('South Bend'),
  ('St. Augustine'),
  ('St. Louis'),
  ('Studio City'),
  ('Tempe'),
  ('Torrance'),
  ('Tucson'),
  ('Ventura'),
  ('Wallingford'),
  ('Washington, D.C.'),
  ('West Chester'),
  ('Wilkes-Barre'),
  ('Winston-Salem'),

  -- UK cities from your list
  ('London'),
  ('Manchester'),
  ('Liverpool'),
  ('Leeds'),
  ('Sheffield'),
  ('Birmingham'),
  ('Nottingham'),
  ('Leicester'),
  ('Derby'),
  ('Coventry'),
  ('Wolverhampton'),
  ('Brighton'),
  ('Bristol'),
  ('Blackburn'),
  ('Blackpool'),
  ('Burnley'),
  ('Cambridge'),
  ('Cardiff'),
  ('Chesterfield'),
  ('Coatbridge'),
  ('Croydon'),
  ('Cumbria'),
  ('Edinburgh'),
  ('Fleetwood'),
  ('High Wycombe'),
  ('Huddersfield'),
  ('Islington'),
  ('Leicester'),
  ('Macclesfield'),
  ('Middlesbrough'),
  ('Newcastle'),
  ('Newcastle upon Tyne'),
  ('Northampton'),
  ('Norwich'),
  ('Pontypridd'),
  ('Preston'),
  ('Ramsgate'),
  ('Reading'),
  ('Royal Leamington Spa'),
  ('Sheerness'),
  ('Southend-on-Sea'),
  ('Southport'),
  ('St. Helens'),
  ('Sterling'),
  ('Stratford'),
  ('Sunderland'),
  ('Taplow'),
  ('Wednesbury'),
  ('Welwyn Garden City'),
  ('Worcester'),
  ('York'),

  -- Canada (English-dominant from your list)
  ('Toronto'),
  ('Vancouver'),
  ('Kelowna'),
  ('Kingston'),
  ('Kitchener'),
  ('Oakville'),
  ('Peterborough'),
  ('New Brunswick'),

  -- Australia
  ('Sydney'),
  ('Melbourne'),
  ('Brisbane'),
  ('Perth'),
  ('Canberra'),
  ('Victoria'),
  ('Western Australia'),

  -- New Zealand
  ('Auckland'),
  ('Wellington'),
  ('Dunedin')
) AS v(area)
WHERE NOT EXISTS (
  SELECT 1
  FROM english_speaking_areas e
  WHERE e.area = v.area
);

CREATE OR REPLACE VIEW top100_for_years_with_lyrics_eng AS
SELECT
  recording_mbid,
  title,
  artist,
  area,
  release_year,
  rating,
  rating_count,
  id
FROM (
  SELECT
    rwr.recording_mbid,
    rwr.title,
    rwr.artist,
    rwr.area,
    rwr.release_year,
    rwr.rating,
    rwr.rating_count,
    ly.id,
    ROW_NUMBER() OVER (
      PARTITION BY rwr.release_year
      ORDER BY rwr.rating_count DESC NULLS LAST
    ) AS rn
  FROM recordings_with_rating rwr
  JOIN lyrics ly
    ON ly.title = rwr.title
   AND ly.artist = rwr.artist
  WHERE rwr.release_year BETWEEN 1976 AND 2017
    AND rwr.area IN (SELECT area FROM english_speaking_areas)
    and ly.id is not null
)
WHERE rn <= 100;

Select release_year, count(release_year) from
recordings_with_rating group by release_year

-- Some check if everything went correcty --
Select distinct release_year from top100_for_years_with_lyrics_eng

Select * from top100_for_years_with_lyrics_eng

-- Records without lyrics 1440
Select count(*) from top100_for_years_with_lyrics_eng where id is null
-- Every year has lyrics with mostly more than 50 lyrics existing the years 1976, 2015, 2017 seem to have more missing ones
Select release_year, count(release_year) from top100_for_years_with_lyrics_eng group by release_year
--------------------------------------------------

-- Create table with just needed lyrics for performance, maybe key on id would be enough
Create or replace table needed_lyrics_eng as 
Select * from lyrics_with_lyrics
where id in (Select id from top100_for_years_with_lyrics_eng)


-- Main Table with analyzed songs
CREATE OR REPLACE TABLE prof_words_per_song_with_info_eng_by_id AS
WITH hits AS (
  SELECT
    l.id AS lyric_id,
    p.id AS profanity_id,
    p.severity,
    p.tags,
    array_length(
      regexp_extract_all(
        lower(l.lyrics),
        CASE
          WHEN p.allow_partial THEN lower(p.id)
          ELSE '\\b(' || lower(p.id) || ')\\b'
        END
      )
    ) AS occurrences
  FROM needed_lyrics_eng l
  CROSS JOIN profanity p
),
filtered AS (
  SELECT *
  FROM hits
  WHERE occurrences > 0
)
SELECT
  lyric_id AS id,

  -- total profane matches across all patterns
  SUM(occurrences) AS total_profanity_count,

  -- sum of (occurrences * severity)
  SUM(occurrences * severity) AS severity_weighted_total,

  -- weighted average severity across matches
  CASE
    WHEN SUM(occurrences) = 0 THEN NULL
    ELSE CAST(SUM(occurrences * severity) AS DOUBLE) / SUM(occurrences)
  END AS severity_avg,

  -- unique list of profanity ids that appeared
  list_distinct(list(profanity_id)) AS profanities_in_lyrics,

  -- all tags that appeared (unique)
  list_distinct(list_concat(list(tags))) AS tags_in_lyrics

FROM filtered
GROUP BY lyric_id;

-- General Information about whole year
create or replace view top_100_with_info_eng as
Select t100.title, t100.artist, t100.release_year, pwp.total_profanity_count, pwp.severity_avg, pwp.tags_in_lyrics, pwp.profanities_in_lyrics from top100_for_years_with_lyrics_eng t100
left join prof_words_per_song_with_info_eng_by_id pwp
on pwp.id = t100.id
and t100.id is not NULL 
order by t100.release_year;

-- Check
Select * from top_100_with_info_eng


-- Tags by amount per year buildup
CREATE OR REPLACE VIEW profanity_tags_per_song_year AS
SELECT
  t100.release_year,
  unnest(p.tags_in_lyrics) AS tag,
  p.id AS lyric_id
FROM prof_words_per_song_with_info_eng_by_id p
left join
top100_for_years_with_lyrics_eng t100
on p.id = t100.id
WHERE p.tags_in_lyrics IS NOT NULL;

--Check 
Select * from profanity_tags_per_song_year


-- Count the Tags
CREATE OR REPLACE VIEW profanity_tag_counts_by_year AS
SELECT
  release_year,
  tag,
  COUNT(DISTINCT lyric_id) AS songs_with_tag
FROM profanity_tags_per_song_year
GROUP BY release_year, tag
ORDER BY release_year, tag;

-- Check
Select * from profanity_tag_counts_by_year

-- Tag amount by year 
CREATE OR REPLACE VIEW profanity_tag_counts_by_year AS
SELECT
  release_year,
  tag,
  COUNT(DISTINCT lyric_id) AS songs_with_tag
FROM profanity_tags_per_song_year
GROUP BY release_year, tag
ORDER BY release_year, tag;

-- Check
Select * from profanity_tag_counts_by_year

-- Density over years
CREATE OR REPLACE VIEW profanity_density_by_year AS
SELECT
  l.release_year,
  AVG(p.total_profanity_count * 1.0 / NULLIF(word_count, 0)) AS avg_density,
  AVG(word_count)
FROM prof_words_per_song_with_info_eng_by_id p
JOIN top100_for_years_with_lyrics_eng_t l ON l.id = p.id
JOIN (
  SELECT id, array_length(string_split(lyrics, ' ')) AS word_count
  FROM needed_lyrics_eng
) wc ON wc.id = l.id
GROUP BY l.release_year
ORDER BY l.release_year;

-- Check
Select * from profanity_density_by_year