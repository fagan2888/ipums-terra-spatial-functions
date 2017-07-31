/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/

-- DROP FUNCTION IF EXISTS _tp_continuous_summarization(bigint, bigint);

CREATE OR REPLACE FUNCTION _tp_continuous_summarization( sample_table_name text, raster_variable_id bigint) 
RETURNS TABLE (geog_instance_id bigint, geog_instance_label text, code bigint, min double precision, max double precision, mean double precision, count bigint ) AS

$BODY$

    DECLARE

    data_raster text := '';
    raster_bnd text := '';
    query text := '';

    BEGIN

    SELECT schema || '.' || tablename as tablename
    FROM rasters_metadata_view rmw
    INTO data_raster
    WHERE rmw.id = raster_variable_id;

    RAISE NOTICE '%', data_raster;

    SELECT band_num
    FROM rasters_metadata_view rmw
    INTO raster_bnd
    WHERE rmw.id = raster_variable_id;

    RAISE NOTICE 'band: %', raster_bnd;


    query  := $$ WITH geographic_boundaries AS 
    (
    SELECT sample_geog_level_id, geog_instance_id, geog_instance_label, geog_instance_code, geom
    FROM $$ || sample_table_name || $$
    ),
    data_rast AS
    (
    SELECT p.geog_instance_id as geog_id, p.geog_instance_label as place_name, p.geog_instance_code as place_code, ST_Clip(r.rast, $$ || raster_bnd || $$, p.geom, ST_BandNoDataValue(r.rast) ) as rast
    FROM geographic_boundaries p inner join $$ || data_raster || $$  r on ST_Intersects(r.rast, p.geom)
    ), summary_rast as
    (
    SELECT d.geog_id, d.place_name, d.place_code, (ST_SummaryStatsAgg(d.rast, 1, True)).*
    FROM data_rast d 
    GROUP BY d.geog_id, d.place_name, d.place_code
    )
    SELECT geog_id, place_name::text, place_code, min, max, mean, count
    FROM summary_rast $$;


RAISE NOTICE  ' % ', query;
RETURN QUERY execute query;

END;

$BODY$
LANGUAGE plpgsql VOLATILE
COST 100
ROWS 1000;

-- SELECT * FROM terrapop_continous_summarization(76, 55);