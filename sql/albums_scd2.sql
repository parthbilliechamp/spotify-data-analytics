MERGE INTO albums AS target
USING staging_albums AS source
ON target.album_id = source.album_id

WHEN MATCHED
AND source.album_name <> target.album_name
AND source.album_release_date <> target.album_release_date
THEN
    UPDATE SET
        target.album_name = source.album_name,
        target.album_release_date = source.album_release_date

WHEN NOT MATCHED
THEN
    INSERT (album_id, album_name, album_release_date)
    VALUES (source.album_id, source.album_name, source.album_release_date);
