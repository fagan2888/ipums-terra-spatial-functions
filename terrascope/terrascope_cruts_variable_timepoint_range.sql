/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/

CREATE OR REPLACE FUNCTION terrascope_cruts_variable_timepoint_range(
    IN raster_mnemonic text,
    IN timepoint text)
  RETURNS TABLE(min double precision, max double precision) AS
$BODY$

    DECLARE
        cruts_mnemonic text := '';
        query text := '';

    BEGIN

    SELECT netcdf_mnemonic
    INTO cruts_mnemonic
    FROM raster_variables
    WHERE mnemonic = raster_mnemonic;

    query  := $$  WITH cruts_time as
    (
    select pixel_id, time, 
    CASE WHEN $$ || cruts_mnemonic || $$  = '-9999' THEN NULL ELSE $$ || cruts_mnemonic || $$ END as cruts_value
    From climate.cruts_322
    where time = '$$ ||timepoint|| $$'
    )
    SELECT min(cruts_value) as min, max(cruts_value) as max
    FROM cruts_time $$;

    RAISE NOTICE  ' % ', query;
    RETURN QUERY execute query;

    END;

$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100
  ROWS 1000;
