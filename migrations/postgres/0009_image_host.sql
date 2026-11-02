-- Gracenote's programme artwork moved off zap2it.tmsimg.com (the hostname no
-- longer resolves) to zpmc.tmsimg.com, which serves the same /assets/ paths.
-- Rewrite the URLs already stored so existing guide data shows artwork without
-- waiting for every window to be fetched again. Safe to re-run: it only
-- touches rows that still have the old host.
UPDATE images
   SET source_url = replace(source_url, 'https://zap2it.tmsimg.com/', 'https://zpmc.tmsimg.com/')
 WHERE source_url LIKE 'https://zap2it.tmsimg.com/%';

UPDATE observations
   SET image_url = replace(image_url, 'https://zap2it.tmsimg.com/', 'https://zpmc.tmsimg.com/')
 WHERE image_url LIKE 'https://zap2it.tmsimg.com/%';
