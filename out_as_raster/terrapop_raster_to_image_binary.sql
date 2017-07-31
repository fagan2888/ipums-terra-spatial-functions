/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/

CREATE OR REPLACE FUNCTION terrapop_raster_to_image_binary( IN data_raster text, IN rasters_id bigint, IN raster_bnd integer DEFAULT 1)
RETURNS TABLE(img bytea) AS
	
	$BODY$

	DECLARE
		query text := '';
	BEGIN

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
	SELECT sample_geog_level_id, geog_instance_id, geog_instance_label, geog_instance_code, geom
	FROM terrapop_clip_boundary
	)
	SELECT ST_AsTIFF(ST_Reclass(ST_Clip(r.rast, $$ || raster_bnd || $$ ,p.geom, ST_BandNoDataValue(r.rast), TRUE), 1, l.exp, '8BUI', ST_BandNoDataValue(r.rast)), ARRAY[1], 'LZW', prj.srid ) as  img
	FROM lookup l, projection prj, polygon p inner join $$ || data_raster || $$ r on ST_Intersects(r.rast,p.geom) $$;

	RETURN QUERY execute query;

    END;
    $BODY$
    
LANGUAGE plpgsql VOLATILE
COST 100
ROWS 1000;