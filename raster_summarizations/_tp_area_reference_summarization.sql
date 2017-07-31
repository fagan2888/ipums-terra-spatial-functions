/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/

-- DROP FUnction _tp_area_reference_summarization(text, bigint, bigint)

CREATE OR REPLACE FUNCTION _tp_area_reference_summarization( sample_table_name text, raster_variable_id bigint) 
RETURNS TABLE (geog_id bigint, place_name text, place_code bigint, total_area double precision) AS

$BODY$

    DECLARE

    area_raster text := '';
    projection integer;	
    query text := '';

    BEGIN

    SELECT schema || '.' || tablename as tablename
    FROM rasters_metadata_view rmw
    INTO area_raster
    WHERE rmw.id = raster_variable_id;

    RAISE NOTICE '%', area_raster;

    SELECT srid
    FROM rasters_metadata_view rmw
    INTO projection
    WHERE rmw.id = raster_variable_id;

    IF projection = 4326 THEN

	    query  := $$  WITH geographic_boundaries as
	    (
	    SELECT sample_geog_level_id, geog_instance_id, geog_instance_label, geog_instance_code, geom
	    FROM $$ || sample_table_name || $$
	    )
	    SELECT p.geog_instance_id as geog_id, p.geog_instance_label::text as place_name, p.geog_instance_code as place_code,  
	    (ST_SummaryStatsAgg(ST_Clip(r.rast, p.geom),1, True)).sum as total_area
	    FROM geographic_boundaries p inner join $$ || area_raster || $$  r on ST_Intersects(r.rast, p.geom)
	    GROUP BY geog_instance_id, geog_instance_label, geog_instance_code $$ ;

	    RAISE NOTICE  ' % ', query;
	   

    ELSE
	    -- Because the NODataValues are not yet set on the area reference rasters the ST_CLIP thing goes crazy if you try to set the nodata value. There should not be a no datavalue
	    query  := $$  WITH geographic_boundaries as
	    (
	    SELECT sample_geog_level_id, geog_instance_id, geog_instance_label, geog_instance_code, geom
	    FROM $$ || sample_table_name || $$
	    ), grouping as
	    (
	    SELECT p.geog_instance_id as geog_id, p.geog_instance_label::text as place_name, p.geog_instance_code as place_code,  
	    (ST_SummaryStatsAgg(ST_Clip(r.rast, 1, p.geom, 0),1, True)).count as pixel_count
	    FROM geographic_boundaries p inner join $$ || area_raster || $$  r on ST_Intersects(r.rast, p.geom)
	    GROUP BY geog_instance_id, geog_instance_label, geog_instance_code 
	    )
	    SELECT geog_id, place_name, place_code, pixel_count * 214658.671875:: double precision as total_area
	    FROM grouping $$;

	    RAISE NOTICE  ' % ', query;


    END IF;
	   

	

    RETURN QUERY execute query;

    END;

$BODY$
LANGUAGE plpgsql VOLATILE
COST 100
ROWS 1000;