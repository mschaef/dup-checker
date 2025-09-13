DELETE FROM file
  WHERE catalog_id in (
    SELECT catalog_id
      FROM catalog
     WHERE catalog_type_id = (
        SELECT catalog_type_id
          FROM catalog_type
         WHERE catalog_type = 'gphoto'));

DELETE FROM catalog
  WHERE catalog_type_id = (
    SELECT catalog_type_id
      FROM catalog_type
     WHERE catalog_type = 'gphoto');

DELETE FROM catalog_type
  WHERE catalog_type = 'gphoto';

DROP TABLE gphoto_media_item;
