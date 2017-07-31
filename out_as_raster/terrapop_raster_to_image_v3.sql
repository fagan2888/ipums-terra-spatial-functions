/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/

-- Function: terrapop_raster_to_image(bigint[], bigint, integer)

-- DROP FUNCTION terrapop_raster_to_image_dh(bigint, bigint, integer);

CREATE OR REPLACE FUNCTION terrapop_raster_to_image_v3(
    IN sample_geog_level_id bigint,
    IN raster_id bigint,
    IN raster_bnd integer DEFAULT 1)
  RETURNS TABLE(img bytea) AS
$BODY$

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

      --STEP 1
      SELECT id, mnemonic, schema, tablename, schema || '.' || tablename as data_rast, band_num, srid, lower(variable_type_description) as type 
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
    $BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100
  ROWS 1000;

