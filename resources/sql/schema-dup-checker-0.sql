CREATE CACHED TABLE catalog (
  catalog_id BIGINT IDENTITY,
  name varchar(32) UNIQUE,
  created_on TIMESTAMP NOT NULL,
  updated_on TIMESTAMP NOT NULL
);

CREATE CACHED TABLE file (
  file_id BIGINT IDENTITY,
  catalog_id BIGINT NOT NULL REFERENCES catalog(catalog_id),
  name VARCHAR(1024),
  extension VARCHAR(256) NOT NULL,
  size BIGINT NOT NULL,
  last_modified_on TIMESTAMP NOT NULL,
  md5_digest VARCHAR(128) NOT NULL,
  sha256_digest VARCHAR(128) NULL
);

