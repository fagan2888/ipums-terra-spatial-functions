/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/

CREATE OR REPLACE FUNCTION terrapop_categorical_modis_summarization_fast( sample_geog_level_id bigint, raster_variable_id bigint, raster_bnd bigint) 
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


    query := $$ WITH raster_projection AS
    (
    select st_srid(rast) as prj
    from $$ || data_raster || $$ 
    limit 1
    ), geographic_boundaries AS 
    (
    SELECT sgl.id as sample_geog_level_id, gi.id as geog_instance_id, gi.label as geog_instance_label, gi.code as geog_instance_code, ST_Transform(bound.geom, prj.prj) as geom
    FROM raster_projection prj,
    sample_geog_levels sgl
    inner join geog_instances gi on sgl.id = gi.sample_geog_level_id
    inner join boundaries bound on bound.geog_instance_id = gi.id
    WHERE sgl.id = $$ || sample_geog_level_id || $$
    )
    ,cat_rast AS
    (
    SELECT p.geog_instance_id as geog_id, p.geog_instance_label as place_name, p.geog_instance_code as place_code, ST_Clip(r.rast, $$ || raster_bnd || $$, p.geom, 0) as rast
    FROM geographic_boundaries p inner join $$ ||  data_raster || $$ r on ST_Intersects(r.rast, p.geom)
    order by 1
    ), valuecount_rast  as
    (
    SELECT geog_id, place_name, place_code, (ST_valuecount(rast)).*
    FROM cat_rast 
    )
    , distinct_categories as
    (
    SELECT geog_id, place_name, place_code, value as categories, sum(count) as num_pixels
    -- num_pixel field is not being use, but Josh could use it get the histogram for a specific place
    FROM valuecount_rast
    GROUP BY geog_id, place_name, place_code, value
    ), number_categories as
    (
    select geog_id, place_name, place_code, count(categories) as num_categories
    from distinct_categories
    group by geog_id, place_name, place_code
    )
    , mode_categories as
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
