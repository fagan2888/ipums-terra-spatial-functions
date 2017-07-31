/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/

-- DROP FUNCTION IF EXISTS terrapop_gli_harvested_summarization(bigint, bigint);

CREATE OR REPLACE FUNCTION _tp_gli_harvested_summarization( sample_table_name text, raster_variable_id bigint) 
RETURNS TABLE (geog_instance_id bigint, geog_instance_label text, code bigint, percent_area double precision, total_area double precision) AS

$BODY$

    DECLARE

    data_raster text := '';
    area_raster text := '';
    raster_bnd text := '';
    query text := '';
    nodatavalue integer;

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

    WITH t1 as
    (
    SELECT schema || '.' || tablename as tablename, area_reference_id
    FROM rasters_metadata_view rmw
    WHERE rmw.id = raster_variable_id
    )
    SELECT rmw.schema || '.' || rmw.tablename as area_reference_table
    INTO area_raster
    from rasters_metadata_view rmw, t1
    where rmw.id = t1.area_reference_id;


    query  := $$  WITH geographic_boundaries as
    (
    SELECT sample_geog_level_id, geog_instance_id, geog_instance_label, geog_instance_code, geom
    FROM $$ || sample_table_name || $$
    ), data_rast AS
    (
    SELECT p.geog_instance_id as geog_id, p.geog_instance_label as place_name, p.geog_instance_code as place_code, ST_Clip(r.rast, $$ || raster_bnd || $$, p.geom, ST_BandNoDataValue(r.rast)) as rast
    FROM geographic_boundaries p inner join $$ || data_raster || $$  r on ST_Intersects(r.rast, p.geom)
    ), area_rast AS
    (
    SELECT p.geog_instance_id as geog_id, p.geog_instance_label as place_name, p.geog_instance_code as place_code, ST_Clip(r.rast, 1, p.geom, 0) as rast
    FROM geographic_boundaries p inner join $$ || area_raster || $$  r on ST_Intersects(r.rast, p.geom)
    ),calc_rast AS
    (
    -- Right now we are trusting that the MapAlgebra function knows what it is doing. The MapAlgebra default uses the intersection.
    -- Using an overlaps/withins or left joins affect how MapAlgebra works.
    -- See ticket #12069
    SELECT d.geog_id, d.place_name, d.place_code, 
    (ST_SummaryStatsAgg(ST_MapAlgebra(d.rast, 1, a.rast, 1, '[rast1]*[rast2]', '32BF'),1, True)).sum as unit_area, 
    (ST_SummaryStatsAgg(a.rast, 1, True)).sum as total_area
    FROM data_rast d inner join area_rast a on (d.geog_id = a.geog_id)
    GROUP BY d.geog_id, d.place_name, d.place_code
    )
    SELECT geog_id, place_name::text, place_code, unit_area/total_area as percent_area, unit_area
    -- total_area
    FROM calc_rast  $$ ;

    RAISE NOTICE  ' % ', query;
    RETURN QUERY execute query;

    END;

$BODY$
LANGUAGE plpgsql VOLATILE
COST 100
ROWS 1000;

-- SELECT * FROM terrapop_gli_harvested_summarization(76, 267);