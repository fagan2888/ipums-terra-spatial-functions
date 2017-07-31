This file provides a brief description of the out as raster functions currently utilized for TerraPop.


There are two classes of functions based on the storage mechanism.
1) Rasters stored in PostgreSQL 
These functions will run through the following function terrapop_raster_to_image_v3. It follows a structure similar to that of the raster summary functions in that there are sub functions. The result is a table that must be extracted by ruby. See the terraclip_outdata.py script for producing a tiff file on your own computer.

2) Rasters stored in Geoserver.
This is actually the functionality that we would rather use as it much more straight forward and Geoserver does all the hard work. The images from geoserver are only the extent of the bounding box of the shapefile. Right now with PostgreSQL they extend to the extent of the intersected tiles. Problem is we can't support native MODIS projection in geoserver. This function is more straight forward in that you just request from geoserver the image and then apply the mask to it. The WCS request bounding box must be the same as its spatial resolution. For CRU TS everything should be on the .5 degrees. You must translate the coordinate using the geotransform to correct location. That's where there is the world2pixel and then pixel2world. We get the row colum of the bounding box and then figure out the top left and bottom right of that bounding box in CRUTS coordinates. After that the most complex thing is the request.



Weird thought: You could try wrapping the output of class 1 and ramming it into class 2 to trim things, but it seems difficult without having the geoTransform loaded in the database properly. That would improve things.

DECLARE
    data_raster text := '';
    raster_type text := '';
    rasters_schema text := '';
    raster_variable_metadata record;
    query text := '';


BEGIN

-- Every raster_variable_id goes through this query.. 
-- Step 1: Get Information about the raster, raster_variable_type, raster_table, and nodataValue
-- Step 2: Verify Boundary Geometry
-- Step 3
-- Determine if the raster_variable is categorical or Binary,
-- If Binary and go through the reclassification steps the output raster data type is '8BUI'  8-bit unsigned integer
-- If you are not Binary you are ELSE, 


--I like these record types. They are like object tables within function. You can refere to there fields using the dot notation
--STEP 1
SELECT id, mnemonic, schema, tablename, schema || '.' || tablename as data_rast, band_num, srid, lower(mnemonic_type) as type 
INTO raster_variable_metadata
FROM rasters_metadata_view
WHERE id = raster_id;


DROP TABLE IF EXISTS terrapop_clip_boundary;

--STEP 2
query := $$ CREATE TEMP TABLE terrapop_clip_boundary AS
WITH raster_projection AS
(
select st_srid(rast) as prj
from $$ || raster_variable_metadata.data_rast || $$
limit 1
)
SELECT sgl.id as sample_geog_level_id, gi.id as geog_instance_id, gi.label as geog_instance_label, gi.code as geog_instance_code, ST_Transform(bound.geom, prj.prj) as geom,
ST_IsValidReason(ST_Transform(bound.geom, prj.prj)) as reason
FROM raster_projection prj,
sample_geog_levels sgl
inner join geog_instances gi on sgl.id = gi.sample_geog_level_id
inner join boundaries bound on bound.geog_instance_id = gi.id
WHERE sgl.id = $$ || sample_geog_level_id || $$ $$;

RAISE NOTICE  ' % ', query;

EXECUTE query;

-- Helpful information
-- http://postgis.net/docs/ST_CollectionExtract.html
-- http://gis.stackexchange.com/questions/157091/cleaning-geometries-in-postgis
-- The ST_MakeValid works fairly well, but it results in a geometry collection, not a geometry that has multiple feature (points, lines, polygons) This is why the error occured on fiji

Update terrapop_clip_boundary
SET geom = ST_CollectionExtract(ST_MakeValid(geom),3), reason = ST_IsValidReason(ST_MakeValid(geom))
WHERE reason <> 'Valid Geometry';

DELETE FROM terrapop_clip_boundary
WHERE ST_IsValidReason(geom) <> 'Valid Geometry';

-- Unlike summary stats, where a null value maybe appropriate, we need to raise a big error if we can't create a valid geometry.

-- Ensure the table has at least 1 valid geometry record
IF (select count(1) from terrapop_clip_boundary) > 0 THEN

RAISE NOTICE  'Boundary has at least 1 valid geography';

IF raster_variable_metadata.type = 'binary' THEN
  RETURN QUERY SELECT * FROM terrapop_raster_to_image_binary(raster_variable_metadata.data_rast, raster_id , raster_bnd );        
ELSE
  RETURN QUERY SELECT * FROM terrapop_raster_to_image_nonbinary(raster_variable_metadata.data_rast, raster_id , raster_bnd );
END IF;

ELSE

RAISE EXCEPTION 'Boundary has no valid geometries... not processing';

END IF;

END;


The inner function does the following for binary

WITH second_area_reference as
(
SELECT second_area_reference_id
FROM rasters_metadata_view rmdv
WHERE rmdv.id = rasters_id
)
SELECT DISTINCT schema || '.' || tablename as table_name
into data_raster
FROM rasters_metadata_view rmdv
inner join second_area_reference on rmdv.id = second_area_reference.second_area_reference_id;


query := $$ WITH lookup AS
(
-- This is Alex magic and you shouldn't change it.
SELECT replace(replace(array_agg(classification::text || ':1')::text, '{', ''), '}', '') as exp
FROM raster_variables WHERE id IN (
    select raster_variable_classifications.mosaic_raster_variable_id
    from raster_variable_classifications
    where raster_variable_classifications.raster_variable_id = $$ || rasters_id || $$ )
),projection as
(
SELECT ST_SRID(r.rast) as srid
FROM $$ || data_raster || $$ r
LIMIT 1
), polygon as
(
SELECT sample_geog_level_id, geog_instance_id, geog_instance_label, geog_instance_code, geom
FROM terrapop_clip_boundary
)
-- This is complex, but it works should be very similar for binary and non-binary.
-- Need the band #
-- Want to preserve the nodata value
-- We are still reclassing so we need that text (e.g. l.exp --> 14:1)
-- Still preserving the nodata value for the output TIFF (record)
-- Add some compression  - 'LZW'
-- Set the projection 

SELECT ST_AsTIFF(ST_Reclass(ST_Clip(r.rast, $$ || raster_bnd || $$ ,p.geom, ST_BandNoDataValue(r.rast), TRUE), 1, l.exp, '8BUI', ST_BandNoDataValue(r.rast)), ARRAY[1], 'LZW', prj.srid ) as  img
FROM lookup l, projection prj, polygon p inner join $$ || data_raster || $$ r on ST_Intersects(r.rast,p.geom) $$;

RETURN QUERY execute query;

