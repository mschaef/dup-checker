
CREATE CACHED TABLE file (
  file_id BIGINT IDENTITY,
  filename VARCHAR(1024) UNIQUE,
  md5_digest VARCHAR(128),
  sha256_digest VARCHAR(128),
);

