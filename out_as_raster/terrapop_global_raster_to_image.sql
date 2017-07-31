/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/

-- DROP FUNCTIon terrapop_global_raster_to_image(bigint, integer)

CREATE OR REPLACE FUNCTION terrapop_global_raster_to_image( IN raster_id bigint, IN raster_bnd integer DEFAULT 1)
  RETURNS TABLE(img bytea) AS
$BODY$

      DECLARE

        raster_variable_metadata record;
        query text := '';
            
      BEGIN

      -- Every raster dataset stored in PostgreSQL goes through this query. NOT netcdf
      -- Step 1: Get Information about the raster, raster_variable_type, raster_table, and nodataValue
      -- Determine if the raster_variable is categorical or Binary,
      -- If Binary and go through the reclassification steps the output raster data type is '8BUI'  8-bit unsigned integer
      -- If you are not Binary you are ELSE, 

      --STEP 1
      SELECT id, mnemonic, schema, tablename, schema || '.' || tablename as data_rast, band_num, srid, lower(variable_type_description) as type 
      INTO raster_variable_metadata
      FROM rasters_metadata_view
      WHERE id = raster_id;

         
      IF raster_variable_metadata.type = 'binary' THEN
        query := $$
        WITH lookup AS
        (
        SELECT replace(replace(array_agg(classification::text || ':1')::text, '{', ''), '}', '') as exp
        FROM raster_variables WHERE id IN (
            select raster_variable_classifications.mosaic_raster_variable_id
            from raster_variable_classifications
            where raster_variable_classifications.raster_variable_id = $$ || raster_variable_metadata.id || $$ )
        )
        SELECT ST_AsTIFF(ST_Reclass(r.rast,  $$ || raster_bnd || $$ , l.exp, '8BUI',  ST_BandNoDataValue(r.rast)), ARRAY[1], 'LZW', ST_SRID(r.rast) ) as  img
        FROM lookup l, $$ || raster_variable_metadata.data_rast || $$ r  $$ ;

        RETURN QUERY execute query;

      ELSE
        query := $$
        SELECT ST_AsTIFF(ST_Band(r.rast, $$ || raster_bnd || $$ ), ARRAY[1], 'LZW', ST_SRID(r.rast) ) as  img
        FROM $$ || raster_variable_metadata.data_rast || $$ r  $$;

        RETURN QUERY execute query;

      END IF;

      END;
    $BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100
  ROWS 1000;

