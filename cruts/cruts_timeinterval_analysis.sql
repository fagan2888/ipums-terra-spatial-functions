/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/

-- DROP FUNCTION terrapop_cruts_timeinterval_analysis(bigint, bigint, text, text, date, date)

CREATE OR REPLACE FUNCTION terrapop_cruts_timeinterval_analysis(sample_geog_level_id bigint, raster_var_id bigint, densification_table text, cruts_data_table text, start_time date, end_time date) 
RETURNS TABLE(geog_instance_id bigint, geog_place text, user_date date, cruts_variable_name text, min double precision, max double precision, mean double precision, count bigint ) AS

$BODY$

DECLARE 
    query_string text := '';
    cruts_variable_name text := '';

BEGIN

    SELECT lower(netcdf_mnemonic)
    INTO cruts_variable_name
    FROM raster_variables
    WHERE id = raster_var_id;

    RAISE NOTICE '%', cruts_variable_name;

    query_string := $$ WITH geographic_boundary as
    (
    SELECT bound.id as place_id, bound.description as place, bound.geom
    FROM sample_geog_levels sgl
    inner join geog_instances gi on sgl.id = gi.sample_geog_level_id
    inner join boundaries bound on bound.geog_instance_id = gi.id
    WHERE sgl.id  = $$ || sample_geog_level_id || $$
    ), geographic_cruts as
    (
    SELECT g.place_id, g.place, c.pixel_id, c.geom
    from geographic_boundary g inner join  $$ || densification_table || $$ c on ST_Within(c.geom, g.geom)
    ), cruts_temporal as
    (
    SELECT pixel_id, $$ || cruts_variable_name || $$, time
    FROM $$ || cruts_data_table || $$ c
    WHERE time BETWEEN ' $$ || start_time|| $$ ' and ' $$ || end_time || $$ '
    ),variable_name as
    (
    SELECT '$$ || cruts_variable_name || $$' as name
    ), cruts_aggregate as
    (
    SELECT gc.place_id::bigint, gc.place::text, ct.time as user_date, 
    min(ct.$$ || cruts_variable_name || $$ ) as min, max(ct.$$ || cruts_variable_name || $$) as max, 
    avg(ct.$$ ||cruts_variable_name || $$) as mean, count(ct. $$ || cruts_variable_name || $$) as count
    FROM cruts_temporal ct inner join geographic_cruts gc on ct.pixel_id = gc.pixel_id
    GROUP BY gc.place_id, gc.place, time
    )
    SELECT place_id, place, user_date, name::text as cruts_variable_name, min, max, mean, count
    FROM cruts_aggregate, variable_name $$;

    RETURN QUERY execute query_string;


END;

$BODY$
LANGUAGE plpgsql VOLATILE
COST 100;