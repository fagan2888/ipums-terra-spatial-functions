/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/

CREATE OR REPLACE FUNCTION terrapop_raster_to_image_nonbinary( IN data_raster text, IN rasters_id bigint, IN raster_bnd integer DEFAULT 1)
RETURNS TABLE(img bytea) AS
	
	$BODY$

	DECLARE
		query text := '';

	BEGIN

	query := $$ WITH projection as
	(
	SELECT ST_SRID(rast) as srid
	FROM $$ || data_raster || $$
	Limit 1
	),polygon as
	(
	SELECT sample_geog_level_id, geog_instance_id, geog_instance_label, geog_instance_code, geom
	FROM terrapop_clip_boundary
	),raster_clip as
	(
	SELECT ST_Clip(r.rast, $$ || raster_bnd || $$, p.geom, ST_BandNoDataValue(r.rast), False) AS rast
	FROM polygon p inner join $$ || data_raster || $$  r on ST_Intersects(r.rast,p.geom)
	)
	SELECT ST_AsTIFF(rast, 'LZW', prj.srid) as  img
	FROM raster_clip, projection prj $$ ;

	RETURN QUERY execute query;

    END;
    $BODY$

LANGUAGE plpgsql VOLATILE
COST 100
ROWS 1000;