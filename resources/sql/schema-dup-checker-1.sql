
CREATE CACHED TABLE catalog_type (
  catalog_type_id TINYINT IDENTITY,
  catalog_type VARCHAR(16)
);

INSERT INTO catalog_type VALUES(0, 'fs');
INSERT INTO catalog_type VALUES(1, 's3');

ALTER TABLE catalog
  ADD catalog_type_id INT DEFAULT 0 NOT NULL;

ALTER TABLE catalog
  ADD FOREIGN KEY(catalog_type_id)
  REFERENCES catalog_type(catalog_type_id);
