/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/

CREATE OR REPLACE FUNCTION terrapop_categorial_modis_summarization( sample_geog_level_id bigint, raster_variable_id bigint, raster_bnd bigint) 
RETURNS TABLE (geog_id bigint, place_name text, place_code bigint, mode double precision, num_categories bigint) AS

$BODY$

DECLARE

    data_raster text := '';
    query text := '';

    BEGIN

    SELECT schema || '.' || tablename as tablename
    FROM rasters_metadata_view nw
    INTO data_raster
    WHERE nw.id = raster_variable_id;

    RAISE NOTICE '%', data_raster;


    query := $$ WITH geographic_boundaries AS 
    (
    SELECT sgl.id as sample_geog_level_id, gi.id as geog_instance_id, gi.label as geog_instance_label, gi.code as geog_instance_code, st_transform(bound.geog::geometry,106842) as geom
    FROM sample_geog_levels sgl
    inner join geog_instances gi on sgl.id = gi.sample_geog_level_id
    inner join boundaries bound on bound.geog_instance_id = gi.id
    WHERE sgl.id = $$ || sample_geog_level_id || $$
    ),
    cat_rast AS
    (
    SELECT p.geog_instance_id as geog_id, p.geog_instance_label as place_name, p.geog_instance_code as place_code, ST_Union(ST_Clip(r.rast, $$ || raster_bnd || $$, p.geom, 0)) as rast
    FROM geographic_boundaries p inner join $$ || data_raster || $$  r on ST_Intersects(r.rast, p.geom)
    GROUP BY p.geog_instance_id, p.geog_instance_label, p.geog_instance_code
    ), valuecount_rast  as
    (
    SELECT geog_id, place_name, place_code, (ST_valuecount(rast)).*
    FROM cat_rast 
    ), distinct_categories as
    (
    SELECT geog_id, place_name, place_code, count(value) as categories
    FROM valuecount_rast
    GROUP BY geog_id, place_name, place_code
    ), mode_categories as
    (
    SELECT DISTINCT geog_id, place_name, place_code, first_value(value) OVER w as category, max(count) OVER w as max_num_pixels
    FROM valuecount_rast
    WINDOW w AS ( PARTITION BY geog_id, place_name, place_code ORDER By count DESC)
    )
    SELECT dc.geog_id, dc.place_name::text, dc.place_code, mc.category as mode, dc.categories as num_categories
    FROM distinct_categories dc inner join mode_categories mc on dc.geog_id = mc.geog_id $$;

RAISE NOTICE  ' % ', query;
RETURN QUERY execute query;

END;

$BODY$
LANGUAGE plpgsql VOLATILE
COST 100
ROWS 1000;
