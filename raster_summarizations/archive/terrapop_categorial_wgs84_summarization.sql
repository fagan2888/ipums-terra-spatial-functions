/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/

-- DROP FUNCTION terrapop_categorial_wgs84_summarization(bigint, bigint);

CREATE OR REPLACE FUNCTION _tp_wgs84_categorical_summarization( sample_table_name text, raster_variable_id bigint, raster_bnd bigint) 
RETURNS TABLE (geog_instance_id bigint, geog_instance_label text, code bigint, mode double precision, num_categories bigint) AS

$BODY$

DECLARE

    data_raster text := '';
    raster_bnd text := '';
    query text := '';

    BEGIN

    SELECT schema || '.' || tablename as tablename
    FROM rasters_metadata_view nw
    INTO data_raster
    WHERE nw.id = raster_variable_id;

    RAISE NOTICE '%', data_raster;

    SELECT band_num
    FROM rasters_metadata_view nw
    INTO raster_bnd
    WHERE nw.id = raster_variable_id;



    query := $$ WITH geographic_boundaries AS 
    (
    SELECT sample_geog_level_id, geog_instance_id, geog_instance_label, geog_instance_code, geom
    FROM $$ || sample_table_name || $$
    ),
    cat_rast AS
    (
    SELECT p.geog_instance_id as geog_id, p.geog_instance_label as place_name, p.geog_instance_code as place_code, ST_Clip(r.rast, $$ || raster_bnd || $$, p.geom, 0) as rast
    FROM geographic_boundaries p inner join $$ || data_raster || $$  r on ST_Intersects(r.rast, p.geom)
    ), valuecount_rast  as
    (
    SELECT geog_id, place_name, place_code, (ST_valuecount(rast)).*
    FROM data_rast 
    ), distinct_categories as
    (
    SELECT geog_id, place_name, place_code, value as categories, sum(count) as num_pixels
    FROM valuecount_rast
    GROUP BY geog_id, place_name, place_code, value
    ), number_categories as
    (
    select geog_id, place_name, place_code, count(categories) as num_categories
    from distinct_categories
    group by geog_id, place_name, place_code
    ), mode_categories as
    (
    SELECT DISTINCT geog_id, place_name, place_code, first_value(categories) OVER w as mode_category, max(num_pixels) OVER w as max_num_pixels
    FROM distinct_categories
    WINDOW w AS ( PARTITION BY geog_id, place_name, place_code ORDER By geog_id, num_pixels DESC)
    )
    SELECT nc.geog_id, nc.place_name::text, nc.place_code, mc.mode_category as mode, nc.num_categories
    FROM number_categories nc
    inner join mode_categories mc on nc.geog_id = mc.geog_id $$;

RAISE NOTICE  ' % ', query;
RETURN QUERY execute query;

END;

$BODY$
LANGUAGE plpgsql VOLATILE
COST 100
ROWS 1000;


-- SELECT * from terrapop_categorial_wgs84_summarization(76, 21)