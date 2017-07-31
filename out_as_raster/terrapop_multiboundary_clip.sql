/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/

CREATE OR REPLACE FUNCTION terrapop_raster_to_image( IN sample_geog_lvl_id bigint[], IN rasters_id bigint, IN raster_bnd integer DEFAULT 1)
  RETURNS TABLE(img bytea) AS
$BODY$

      DECLARE
            data_raster text := '';
            raster_type text := '';
            rasters_schema text := '';
            query text := '';

      BEGIN

      -- Every raster_variable_id goes through this query.. Determines if the raster_variable should go Categorical to Binary and go through the reclassification step

      SELECT lower(rmdv.mnemonic_type) as mnemonic_type
      into raster_type
      FROM rasters_metadata_view rmdv
      WHERE  rmdv.id = rasters_id;

      IF raster_type = 'binary' THEN

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
        SELECT sgl.id as sample_geog_level_id, gi.id as geog_instance_id, gi.label as geog_instance_label,
        gi.code as geog_instance_code, ST_Transform(bound.geog::geometry, prj.srid) as geom
        FROM projection prj, sample_geog_levels sgl
        inner join geog_instances gi on sgl.id = gi.sample_geog_level_id
        inner join boundaries bound on bound.geog_instance_id = gi.id
        WHERE sgl.id IN ($$ || array_to_string(sample_geog_lvl_id, ',') || $$)
        )
        SELECT ST_AsTIFF(ST_Reclass(ST_Clip(r.rast, $$ || raster_bnd || $$ ,p.geom,-999, TRUE), 1, l.exp, '8BUI'), ARRAY[1], 'LZW', prj.srid ) as  img
        FROM lookup l, projection prj, polygon p inner join $$ || data_raster || $$ r on ST_Intersects(r.rast,p.geom) $$;

      ELSE

        SELECT schema 
        FROM rasters_metadata_view rmdv
        INTO rasters_schema
        WHERE rmdv.id = rasters_id;

        SELECT schema || '.' || tablename as tablename
        FROM rasters_metadata_view rmdv
        INTO data_raster
        WHERE rmdv.id = rasters_id;

        IF rasters_schema = 'modis' or rasters_schema = 'landcover' THEN

          query := $$ WITH projection as
          (
          SELECT ST_SRID(rast) as srid
          FROM $$ || data_raster || $$
          Limit 1
          ),polygon as
          (
          SELECT sgl.id as sample_geog_level_id, ST_Transform(bound.geog::geometry, prj.srid) as geom
          FROM projection prj, sample_geog_levels sgl
          inner join geog_instances gi on sgl.id = gi.sample_geog_level_id
          inner join boundaries bound on bound.geog_instance_id = gi.id
          WHERE sgl.id IN ($$ || array_to_string(sample_geog_lvl_id, ',') || $$)    
          ),raster_clip as
          (
          SELECT ST_Clip(r.rast, $$ || raster_bnd || $$, p.geom,-999, False) AS rast
          FROM polygon p inner join $$ || data_raster || $$  r on ST_Intersects(r.rast,p.geom)
          )
          SELECT ST_AsTIFF(rast, 'LZW') as  img
          FROM raster_clip, projection prj $$ ;

        ELSE
          query := $$ WITH projection as
          (
          SELECT ST_SRID(rast) as srid
          FROM $$ || data_raster || $$
          Limit 1
          ),rastervariable_band as
          (
          SELECT rmdv.band_num 
          FROM rasters_metadata_view rmdv
          WHERE rmdv.id = $$ || rasters_id || $$
          ),polygon as
          (
          SELECT sgl.id as sample_geog_level_id, ST_Transform(bound.geog::geometry, prj.srid) as geom
          FROM projection prj, sample_geog_levels sgl
          inner join geog_instances gi on sgl.id = gi.sample_geog_level_id
          inner join boundaries bound on bound.geog_instance_id = gi.id
          WHERE sgl.id IN ($$ || array_to_string(sample_geog_lvl_id, ',') || $$)    
          ),raster_clip as
          (
          SELECT ST_Clip(r.rast, rvb.band_num, p.geom,-999, False) AS rast
          FROM rastervariable_band rvb, polygon p inner join $$ || data_raster || $$  r on ST_Intersects(r.rast,p.geom)
          )
          SELECT ST_AsTIFF(rast, 'LZW') as  img
          FROM raster_clip, projection prj $$ ;

        END IF;



      END IF;

      RAISE NOTICE  ' % ', query;
      RETURN QUERY execute query;

      END;
    $BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100
  ROWS 1000;




