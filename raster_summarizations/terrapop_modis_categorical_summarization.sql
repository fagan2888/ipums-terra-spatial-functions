/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/

--  DROP FUNCTION terrapop_categorical_modis_testing_table(bigint, bigint, bigint);
CREATE OR REPLACE FUNCTION terrapop_modis_categorical_summarization( sample_geog_level_id bigint, raster_variable_id bigint, raster_bnd bigint) 
RETURNS TABLE (geog_instance_id bigint, geog_instance_label text, code bigint, mod_class double precision, num_class bigint) AS

$BODY$

DECLARE

    data_raster text := '';
    query text := '';

    BEGIN

    SELECT schema || '.' || tablename as tablename
    FROM rasters_metadata_view rmw
    INTO data_raster
    WHERE rmw.id = raster_variable_id;

    RAISE NOTICE '%', data_raster;

    DROP TABLE IF EXISTS terrapop_modis_boundary;

    query := $$ CREATE TEMP TABLE terrapop_modis_boundary AS
    WITH raster_projection AS
    (
    select st_srid(rast) as prj
    from $$ || data_raster || $$ 
    limit 1
    )
    SELECT sgl.id as sample_geog_level_id, gi.id as geog_instance_id, gi.label as geog_instance_label, gi.code as geog_instance_code, ST_Transform(bound.geom, prj.prj) as geom,
    ST_IsValidReason(ST_Transform(bound.geom, prj.prj)) as reason
    FROM raster_projection prj,
    sample_geog_levels sgl
    inner join geog_instances gi on sgl.id = gi.sample_geog_level_id
    inner join boundaries bound on bound.geog_instance_id = gi.id
    WHERE sgl.id = $$ || sample_geog_level_id || $$ $$;

    RAISE NOTICE  ' % ', query;

    EXECUTE query;

    Update terrapop_modis_boundary
    SET geom = ST_CollectionExtract(ST_MakeValid(geom),3), reason = ST_IsValidReason(ST_MakeValid(geom))
    WHERE reason <> 'Valid Geometry';

    DELETE FROM terrapop_modis_boundary
    WHERE ST_IsValidReason(geom) <> 'Valid Geometry';

    RETURN QUERY SELECT * FROM _tp_modis_categorical_summarization('terrapop_modis_boundary', raster_variable_id, raster_bnd );


END;

$BODY$

LANGUAGE plpgsql VOLATILE
COST 100;


-- select * from terrapop_categorical_modis_summarization(691,2,3)