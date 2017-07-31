/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/

CREATE OR REPLACE FUNCTION terrapop_raster_variable_projection(
    raster_variable_id bigint,
    raster_bnd bigint)
  RETURNS bigint AS
$BODY$

        DECLARE
        query text := '';
        data_raster text := '';
        srid bigint;

        BEGIN
        query := $$
        SELECT rmw.schema || '.' || rmw.tablename as tablename
        FROM rasters_metadata_view rmw
        WHERE rmw.id = $$ || raster_variable_id || $$ $$ ;
        Execute query INTO data_raster;

	IF data_raster IS NULL THEN
		srid := 0;
		RETURN srid;
	ELSE
		query := $$ 
		Select ST_SRID(rast)
		FROM $$ || data_raster || $$ 
		limit 1    $$ ;

		RAISE NOTICE  ' % ', query;
		Execute query INTO srid;
		RETURN srid;
        END IF;

        END;

    $BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;