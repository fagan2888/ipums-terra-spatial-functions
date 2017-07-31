/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/

 WITH t1 as
(
SELECT schema || '.' || tablename as tablename, area_reference_id
FROM rasters_metadata_view rm
WHERE rm.id = 27
)
SELECT DISTINCT rm.schema || '.' || rm.tablename as area_reference_table
-- INTO area_raster
from rasters_metadata_view rm, t1
where rm.id = 27

CREATE OR REPLACE FUNCTION terrapop_continous_summarization( sample_geog_level_id bigint, raster_variable_id bigint) 

SELECT 'Generating SummaryStats for mnemonic: ' || mnemonic || '. QUERYING table: ' || schema || '.' || tablename || ' band:' || band_num as response
FROM rasters_metadata_view 
where id = 472



SELECT replace(replace(array_agg(classification::text || ':1')::text, '{', ''), '}', '') as exp
        FROM raster_variables WHERE id IN (
                select raster_variable_classifications.mosaic_raster_variable_id 
                from raster_variable_classifications
                where raster_variable_classifications.raster_variable_id = 33)