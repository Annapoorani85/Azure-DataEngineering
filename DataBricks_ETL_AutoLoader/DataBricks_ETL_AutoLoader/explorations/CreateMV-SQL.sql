-- Databricks notebook source
-- Define a streaming table to ingest data from a volume
CREATE OR REFRESH STREAMING TABLE songs_raw
COMMENT "Raw data from a subset of the Million Song Dataset; a collection of features and metadata for contemporary music tracks."
AS SELECT *
FROM STREAM read_files(
  '/databricks-datasets/songs/data-001/part*',
  format => "csv",
  header => "false",
  delimiter => "\t",
  schema => """
    artist_id STRING,
    artist_lat DOUBLE,
    artist_long DOUBLE,
    artist_location STRING,
    artist_name STRING,
    duration DOUBLE,
    end_of_fade_in DOUBLE,
    key INT,
    key_confidence DOUBLE,
    loudness DOUBLE,
    release STRING,
    song_hotnes DOUBLE,
    song_id STRING,
    start_of_fade_out DOUBLE,
    tempo DOUBLE,
    time_signature INT,
    time_signature_confidence DOUBLE,
    title STRING,
    year INT,
    partial_sequence STRING
  """,
  schemaEvolutionMode => "none");

-- COMMAND ----------


-- Define a materialized view that validates data and renames a column
CREATE OR REFRESH MATERIALIZED VIEW songs_prepared(
CONSTRAINT valid_artist_name EXPECT (artist_name IS NOT NULL),
CONSTRAINT valid_title EXPECT (song_title IS NOT NULL),
CONSTRAINT valid_duration EXPECT (duration > 0)
)
COMMENT "Million Song Dataset with data cleaned and prepared for analysis."
AS SELECT artist_id, artist_name, duration, release, tempo, time_signature, title AS song_title, year
FROM songs_raw;

-- COMMAND ----------

-- Define a materialized view that has a filtered, aggregated, and sorted view of the data
CREATE OR REFRESH MATERIALIZED VIEW top_artists_by_year
COMMENT "A table summarizing counts of songs released by the artists each year, who released the most songs."
AS SELECT
  artist_name,
  year,
  COUNT(*) AS total_number_of_songs
FROM songs_prepared
WHERE year > 0
GROUP BY artist_name, year
ORDER BY total_number_of_songs DESC, year DESC;