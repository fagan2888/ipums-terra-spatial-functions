/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/

CREATE OR REPLACE FUNCTION _tp_MODIS_categorical_binary_summarization( sample_table_name text, raster_variable_id bigint, raster_bnd bigint) 
RETURNS TABLE (geog_instance_id bigint, geog_instance_label text, code bigint, percent_area double precision, total_area double precision) AS

$BODY$

    DECLARE

    data_raster text := '';
    area_raster text := '';
    query text := '';
    query2 text := '';
    nodatavalue integer;

    BEGIN

    SELECT schema || '.' || tablename as tablename
    FROM rasters_metadata_view rmw
    INTO data_raster
    WHERE rmw.id = raster_variable_id;

    RAISE NOTICE '%', data_raster;

    query := $$ 
    SELECT ST_BandNoDataValue(rast)::integer
    FROM $$ || data_raster || $$ 
    LIMIT 1 $$ ;
    
    RAISE NOTICE  ' % ', query;
    Execute query INTO nodatavalue;

    query := $$ WITH lookup AS
	(
	SELECT replace(replace(array_agg(classification::text || ':1')::text, '{', ''), '}', '') as exp
	FROM raster_variables WHERE id IN (
		select raster_variable_classifications.mosaic_raster_variable_id 
		from raster_variable_classifications
		where raster_variable_classifications.raster_variable_id = $$ || raster_variable_id || $$) 
	), geographic_boundaries as
	(
	SELECT sample_geog_level_id, geog_instance_id, geog_instance_label, geog_instance_code, geom
	FROM $$ || sample_table_name || $$
	), data_rast AS 
	(
	SELECT p.geog_instance_id as geog_id, p.geog_instance_label as place_name, p.geog_instance_code as place_code, ST_Clip(r.rast, $$ || raster_bnd || $$, p.geom, ST_BandNoDataValue(r.rast)) as rast
	FROM lookup l, geographic_boundaries p inner join $$ || data_raster || $$ r on ST_Intersects(r.rast, p.geom)
	), total_group as
	(
	SELECT geog_id, place_name, place_code, sum(ST_Count(d.rast)) as total_pixels
	FROM data_rast d
	GROUP BY geog_id, place_name, place_code
	), binary_pixels as
	(
	SELECT geog_id, place_name, place_code, (ST_ValueCount(ST_Reclass(rast,1, l.exp, '8BUI',0))).*
	FROM data_rast, lookup l
	), binary_group as
	(
	SELECT b.geog_id, b.place_name, b.place_code, sum(b.count) as binary_pixels
	FROM binary_pixels b inner join total_pixels t on b.geog_id = t.geog_id 
	GROUP BY b.geog_id, b.place_name, b.place_code
	)
	SELECT t.geog_id, t.place_name::text, t.place_code, b.binary_pixels/t.total_pixels::double precision as percent_area, b.binary_pixels * 214658.671875:: double precision as total_area
	FROM binary_group b inner join total_group t on b.geog_id = t.geog_id ;
                

    


	RAISE NOTICE  ' % ', query;
	RETURN QUERY execute query;

    END;

$BODY$
LANGUAGE plpgsql VOLATILE
COST 100
ROWS 1000;