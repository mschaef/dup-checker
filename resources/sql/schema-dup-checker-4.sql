
CREATE CACHED TABLE gphoto_media_item (
  entry_id BIGINT IDENTITY,
  gphoto_id VARCHAR(128) NOT NULL,
  name VARCHAR(256) NOT NULL,
  extension VARCHAR(256) NOT NULL,
  mime_type VARCHAR(64) NOT NULL,
  base_url VARCHAR(2048) NOT NULL,
  creation_time TIMESTAMP,
  media_metadata VARCHAR(8192) NOT NULL
);
