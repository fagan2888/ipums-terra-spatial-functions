/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/

-- DROP FUNCTION IF EXISTS terrapop_continous_summarization(bigint, bigint);

CREATE OR REPLACE FUNCTION terrapop_continuous_summarization( sample_geog_level_id bigint, raster_variable_id bigint) 
RETURNS TABLE (geog_instance_id bigint, geog_instance_label text, code bigint, min double precision, max double precision, mean double precision, count bigint ) AS

$BODY$

    DECLARE

    data_raster text := '';
    raster_bnd bigint;
    query text := '';

    BEGIN

    SELECT schema || '.' || tablename as tablename
    FROM rasters_metadata_view nw
    INTO data_raster
    WHERE nw.id = raster_variable_id;

    RAISE NOTICE '%', data_raster;

    SELECT band_num::bigint
    FROM rasters_metadata_view nw
    INTO raster_bnd
    WHERE nw.id = raster_variable_id;

    RAISE NOTICE 'band: %', raster_bnd;


    DROP TABLE IF EXISTS terrapop_continuous_boundary;

    query := $$ CREATE TEMP TABLE terrapop_continuous_boundary AS
    SELECT sgl.id as sample_geog_level_id, gi.id as geog_instance_id, gi.label as geog_instance_label, gi.code as geog_instance_code, bound.geom as geom,
    ST_IsValidReason(bound.geom) as reason
    FROM sample_geog_levels sgl
    inner join geog_instances gi on sgl.id = gi.sample_geog_level_id
    inner join boundaries bound on bound.geog_instance_id = gi.id
    WHERE sgl.id = $$ || sample_geog_level_id || $$ $$;

    RAISE NOTICE  ' % ', query;

    EXECUTE query;

    Update terrapop_continuous_boundary
    SET geom = ST_CollectionExtract(ST_MakeValid(geom),3), reason = ST_IsValidReason(ST_MakeValid(geom))
    WHERE reason <> 'Valid Geometry';

    DELETE FROM terrapop_continuous_boundary
    WHERE ST_IsValidReason(geom) <> 'Valid Geometry';

    RETURN QUERY SELECT * FROM _tp_continuous_summarization('terrapop_continuous_boundary'::text, raster_variable_id );



END;

$BODY$
LANGUAGE plpgsql VOLATILE
COST 100
ROWS 1000;
