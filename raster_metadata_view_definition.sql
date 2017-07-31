/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/

Create view rasters_metadata_view as
WITH loaded_rasters as
(
select lower(r_table_schema) as schema, lower(r_table_name) as tablename, srid
from raster_columns
where r_table_schema <> 'gis_rasters'
), raster_metadata as
(
select rv.id, rv.mnemonic, lower(rtn.schema) as schema, lower(rtn.tablename) as tablename, rb.band_num, 
rv.area_reference_id, rv.second_area_reference_id, rdt.id as variable_type, rdt.code as variable_type_description
from raster_table_names rtn
inner join raster_bands rb on rtn.id = rb.raster_table_name_id
inner join raster_variable_raster_bands rvrb on rb.id = rvrb.raster_band_id
inner join raster_variables rv on rvrb.raster_variable_id = rv.id
inner join raster_data_types rdt on rv.raster_data_type_id = rdt.id
), all_data as
(
SELECT rm.id,rm.mnemonic, rm.schema, rm.tablename, rm.band_num, lr.schema as loaded_schema, lr.tablename as loaded_table, 
lr.srid, rm.area_reference_id, rm.second_area_reference_id, variable_type, variable_type_description
FROM raster_metadata rm left join loaded_rasters lr on (rm.schema = lr.schema and rm.tablename = lr.tablename)
), post_gis_rasters as
(
SELECT id, mnemonic, schema, tablename, band_num, srid, area_reference_id, second_area_reference_id, variable_type, variable_type_description,
CASE WHEN loaded_table <> '' THEN 1 END::boolean as data_exists
FROM all_data
),terrapop_netcdf as
(
select rv.id, rv.mnemonic, lower(rg2.name) as schema, lower(rg.mnemonic) as tablename, NULL::integer as band_num,
4326 as srid, NULL::bigint as area_reference_id,  NULL::bigint as second_area_reference_id, rdt.id as variable_type, rdt.code as variable_type_description
from raster_variables rv
inner join raster_groups rg on rv.raster_group_id = rg.id
inner join raster_groups rg2 on rg.parent_id = rg2.id
inner join raster_data_types rdt on rv.raster_data_type_id = rdt.id
where netcdf_mnemonic IS NOT NULL
), database_tables as
(
SELECT table_schema as schema, table_name as tablename
FROM information_schema.tables
), netcdf_rasters as
(
SELECT id, mnemonic, tp.schema, tp.tablename, band_num, srid, area_reference_id, second_area_reference_id, variable_type, variable_type_description, True as data_exists
FROM terrapop_netcdf tp inner join database_tables db on tp.schema = db.schema and tp.tablename = db.tablename
)
SELECT *
FROM post_gis_rasters
UNION
SELECT *
FROM netcdf_rasters
order by 1