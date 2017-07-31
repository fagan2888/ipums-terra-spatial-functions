/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/

-- DROP FUNCTION terrapop_categorical_modis_testing(bigint, bigint, bigint);
CREATE OR REPLACE FUNCTION terrapop_categorical_modis_testing( sample_geog_level_id bigint, raster_variable_id bigint, raster_bnd bigint) 
RETURNS SETOF text as
-- TABLE (geog_id bigint, place_name text, place_code bigint, mode double precision, num_categories bigint) AS

$BODY$

DECLARE

    data_raster text := '';
    query text := '';
    terrapop_boundaries record;
    rec record;

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
    )
    SELECT sgl.id as sample_geog_level_id, gi.id as geog_instance_id, gi.label as geog_instance_label, gi.code as geog_instance_code, ST_Transform(bound.geom, prj.prj) as geom,
    ST_IsValidReason(ST_Transform(bound.geom, prj.prj)) as reason
    FROM raster_projection prj,
    sample_geog_levels sgl
    inner join geog_instances gi on sgl.id = gi.sample_geog_level_id
    inner join boundaries bound on bound.geog_instance_id = gi.id
    WHERE sgl.id = $$ || sample_geog_level_id || $$ $$;

    RAISE NOTICE  ' % ', query;

    FOR terrapop_boundaries IN EXECUTE query LOOP
  
	RAISE NOTICE  ' % ', terrapop_boundaries.reason;
        IF terrapop_boundaries.reason <> 'Valid Geometry' THEN
            terrapop_boundaries.geom := ST_MakeValid(terrapop_boundaries.geom) ;
	
--             RETURN NEXT terrapop_boundaries.;
        END IF;  
        
    END LOOP;


--     RETURN terrapop_boundaries.reason::text;
END;

$BODY$

LANGUAGE plpgsql VOLATILE
COST 100;


select * from terrapop_categorical_modis_testing(691,2,3)