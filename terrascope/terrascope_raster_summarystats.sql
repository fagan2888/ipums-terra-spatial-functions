/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/

-- DROP FUNCTION terrascope_raster_summarystats(bigint, bigint, text, text)

CREATE OR REPLACE FUNCTION terrascope_raster_summarystats( sample_geog_level_id bigint, raster_id bigint, start_timepoint text, end_timepoint text) 
RETURNS TABLE (geog_instance_id bigint, geog_instance_label text, raster_time_point text, min double precision, max double precision, mean double precision, 
    mode double precision, num_class bigint, percent double precision, area double precision) AS

$BODY$

    DECLARE

    terrapop_raster RECORD;
    terrapop_raster_timepoint record;
    raster_timepoint_description text := '';
    cruts_table_name text := '';
    query text := '';
	

    BEGIN

    SELECT id, mnemonic, band_num, srid, variable_type, variable_type_description, schema || '.' || tablename as raster_data_table
    INTO terrapop_raster
    FROM rasters_metadata_view2
    WHERE id = raster_id;

    RAISE NOTICE '%', terrapop_raster;

    SELECT rv.id, rv.mnemonic, rv.raster_group_id, rt.band, rt.timepoint
    from raster_variables rv 
    INTO terrapop_raster_timepoint
    inner join raster_dataset_raster_variables rdrv on rv.id = rdrv.raster_variable_id
    inner join raster_datasets rd on rdrv.raster_dataset_id = rd.id
    inner join raster_timepoints rt on rt.raster_dataset_id = rd.id
    WHERE rv.id = raster_id; 

    RAISE NOTICE '%', terrapop_raster_timepoint;

    IF terrapop_raster.variable_type = 1 THEN
        -- Returning categorical summary stats
        IF terrapop_raster.srid = 4326 THEN
            RETURN QUERY 
            SELECT t.geog_instance_id, t.geog_instance_label, terrapop_raster_timepoint.timepoint::text, NULL::double precision as min, NULL::double precision as max, NULL::double precision as mean,
            t.mod_class as mode, t.num_class, NULL::double precision as percent, NULL::double precision as area
            FROM terrapop_wgs84_categorical_summarization(sample_geog_level_id, raster_id,  terrapop_raster.band_num) as t;

        ELSEIF
            terrapop_raster.srid = 106842 THEN
            
            -- This query tracks from raster_variables to raster_timepoints and returns the timepoint
            -- Double check this code once Will refactors raster_timepoints

            query := $$ select rv.id, rv.mnemonic, rv.raster_group_id, rt.band, rt.timepoint
            from raster_variables rv
            inner join raster_dataset_raster_variables rdrv on rv.id = rdrv.raster_variable_id
            inner join raster_datasets rd on rdrv.raster_dataset_id = rd.id
            inner join raster_timepoints rt on rt.raster_dataset_id = rd.id
            where rv.id = $$ || raster_id || $$ and rt.timepoint = '$$ || start_timepoint || $$'
            limit 1 $$;

            Execute query INTO terrapop_raster_timepoint;		
            
            RAISE NOTICE '%', terrapop_raster_timepoint.band;
            
            RETURN QUERY 
            SELECT t.geog_instance_id, t.geog_instance_label, terrapop_raster_timepoint.timepoint::text, NULL::double precision as min, NULL::double precision as max, NULL::double precision as mean,
            t.mod_class as mode, t.num_class, NULL::double precision as percent, NULL::double precision as area
            FROM terrapop_modis_categorical_summarization(sample_geog_level_id, raster_id,  terrapop_raster_timepoint.band) as t;

        END IF;

    ELSEIF terrapop_raster.variable_type = 5 THEN
        --Analysing Binary datasets

        IF terrapop_raster.srid = 4326 THEN
            RETURN QUERY 
            SELECT t.geog_instance_id, t.geog_instance_label, terrapop_raster_timepoint.timepoint::text, NULL::double precision as min, NULL::double precision as max, NULL::double precision as mean,
            NULL::double precision as mode, NULL::bigint as num_class, t.percent, t.area
            FROM terrapop_wgs84_categorical_binary_summarization(sample_geog_level_id, raster_id,  terrapop_raster.band_num) as t;

        ELSEIF
            terrapop_raster.srid = 106842 THEN
            
            -- This query tracks from raster_variables to raster_timepoints and returns the timepoint
            -- Double check this code once Will refactors raster_timepoints

            query := $$ select rv.id, rv.mnemonic, rv.raster_group_id, rt.band, rt.timepoint
            from raster_variables rv
            inner join raster_dataset_raster_variables rdrv on rv.id = rdrv.raster_variable_id
            inner join raster_datasets rd on rdrv.raster_dataset_id = rd.id
            inner join raster_timepoints rt on rt.raster_dataset_id = rd.id
            where rv.id = $$ || raster_id || $$ and rt.timepoint = '$$ || start_timepoint || $$'
            limit 1 $$;

            Execute query INTO terrapop_raster_timepoint;       
            
            RAISE NOTICE '%', terrapop_raster_timepoint.band;
            
            RETURN QUERY 
            SELECT t.geog_instance_id, t.geog_instance_label, terrapop_raster_timepoint.timepoint::text, NULL::double precision as min, NULL::double precision as max, NULL::double precision as mean,
            NULL::double precision as mode, NULL::bigint as num_class, t.percent, t.area
            FROM terrapop_MODIS_categorical_binary_summarization(sample_geog_level_id, raster_id,  terrapop_raster_timepoint.band) as t;

        END IF;

    ELSEIF terrapop_raster.variable_type = 2 THEN
        --Analysing all continous raster variables

        RETURN QUERY 
        SELECT t.geog_instance_id, t.geog_instance_label, terrapop_raster_timepoint.timepoint::text, t.min, t.max, t.mean,
        NULL::double precision as mode, NULL::bigint as num_class, NULL::double precision as percent, NULL::double precision as area
        FROM terrapop_continuous_summarization(sample_geog_level_id, raster_id) as t;

    ELSEIF terrapop_raster.variable_type = 3 THEN
        --Analysing all the complex 2 area reference raster dataset
        
        RETURN QUERY 
        SELECT t.geog_instance_id, t.geog_instance_label, terrapop_raster_timepoint.timepoint::text, NULL::double precision as min, NULL::double precision as max, NULL::double precision as mean,
        NULL::double precision as mode, NULL::bigint as num_class, t.percent, t.harvest_area as area
        FROM terrapop_gli_harvested_summarization(sample_geog_level_id, raster_id) as t;


    ELSEIF terrapop_raster.variable_type = 4 THEN
        
        RETURN QUERY 
        SELECT t.geog_instance_id, t.geog_instance_label, terrapop_raster_timepoint.timepoint::text, NULL::double precision as min, NULL::double precision as max, NULL::double precision as mean,
        NULL::double precision as mode, NULL::bigint as num_class, t.percent, t.area
        FROM terrapop_area_reference_summarization(sample_geog_level_id, raster_id) as t;

    ELSEIF terrapop_raster.variable_type = 7 THEN

        SELECT terrapop_get_cruts_template 
        INTO cruts_table_name
        FROM terrapop_get_cruts_template(sample_geog_level_id, raster_id, '/tmp') ;
        
        IF end_timepoint = '' THEN

            -- single timepoint analysis
            RETURN QUERY 
            SELECT t.geog_instance_id, t.geog_place as geog_instance_label, user_date::text, t.min, t.max, t.mean,
            NULL::double precision as mode, NULL::bigint as num_class, NULL::double precision as percent, NULL::double precision as area
            FROM terrapop_cruts_timepoint_analysis(sample_geog_level_id, raster_id, cruts_table_name, terrapop_raster.raster_data_table, start_timepoint::date) as t;

        ELSE

            -- time interval analysis
            RETURN QUERY 
            SELECT t.geog_instance_id, t.geog_place as geog_instance_label, user_date::text, t.min, t.max, t.mean,
            NULL::double precision as mode, NULL::bigint as num_class, NULL::double precision as percent, NULL::double precision as area
            FROM terrapop_cruts_timeinterval_analysis(sample_geog_level_id, raster_id, cruts_table_name, terrapop_raster.raster_data_table, start_timepoint::date, end_timepoint::date) as t;
        END IF;
    END IF;



    END;

$BODY$
LANGUAGE plpgsql VOLATILE
COST 100
ROWS 1000;