
CREATE CACHED TABLE file (
  file_id BIGINT IDENTITY,
  name VARCHAR(1024) UNIQUE,
  size BIGINT,
  md5_digest VARCHAR(128),
  sha256_digest VARCHAR(128)
);

