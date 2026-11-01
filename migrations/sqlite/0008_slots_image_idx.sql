-- Index on slots.image_id. The janitor deletes `images` rows that no slot
-- references (`NOT EXISTS (SELECT 1 FROM slots WHERE image_id = images.id)`)
-- and clears image links by slot age; without this index the "is it still
-- referenced?" check scans the whole slots table once per image row.
CREATE INDEX IF NOT EXISTS slots_image_id_idx ON slots (image_id);
