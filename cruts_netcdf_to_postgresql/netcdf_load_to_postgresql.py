'''
Author: Joshua Donato
Purpose: Extract data from NetCDF files and import into a PostgreSQL database.  The NetCDF files must have the same
data structure, timespan, and extent/resolution (such as the CRU TS data).  The script will include all the variables
in the NetCDF files that are not dimensions.  Dimensions are assumed to be lat, lon, and time.  Optional parameters can
be provided to limit the the data being extracted to a date range and/or geographic regions.

parameters:
    files: array of file paths to the NetCDF files
    host: host for the PostgreSQL database
    dbname: database name for the PostgreSQL database
    user: username for the PostgreSQL database server
    password: password for the PostgreSQL database server
    schema: schema where the data will go
    table_name: table where the data will be saved
    generate_pixel_ids: option to generate pixel ids, defaults to true
    date_start: optional start date for date range, format as "m,d,yyyy", defaults to None
    date_end: optional end date for date range, format as "m,d,yyyy", defaults to None
    shapefile_paths: optional shapefiles to restrict by area, defaults to None

'''


import sys, traceback
import numpy as np
import cStringIO
import psycopg2
from netCDF4 import Dataset
from datetime import datetime
from datetime import timedelta
from shapely.geometry import shape, Point
from shapely.ops import cascaded_union
import fiona
import argparse


#-----Error Handling-------------------------------------------------------------------------------

errorPrinted = False

class CustomException(Exception):
    def __init__(self, value):
        self.parameter = value
    def __str__(self):
        return repr(self.parameter)


def printPyError():
    global errorPrinted
    if errorPrinted == False:
        # Get the traceback object
        tb = sys.exc_info()[2]
        tbinfo = traceback.format_tb(tb)[0]

        # Concatenate information together concerning the error into a message string
        print "PYTHON ERROR:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
        errorPrinted = True


def print_psycopg_error(e):
    global errorPrinted
    if errorPrinted == False:
        # Get the traceback object
        tb = sys.exc_info()[2]
        tbinfo = traceback.format_tb(tb)[0]

        # Concatenate information together concerning the error into a message string
        print "PSYCOPG2 ERROR:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
        errorPrinted = True


def printCustomError(message):
    global errorPrinted
    if errorPrinted == False:

        # Get the traceback object
        tb = sys.exc_info()[2]
        tbinfo = traceback.format_tb(tb)[0]

        msg = "ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info: "
        msg = msg + str(message)
        print msg

        errorPrinted = True


#-----Functions------------------------------------------------------------------------------------


def verifyDateRange(a, b):
    global date_start
    global date_end

    try:
        if date_start:
            date_start = datetime.strptime(date_start, "%m-%d-%Y").date()
        if date_end:
            date_end = datetime.strptime(date_end, "%m-%d-%Y").date()

    except:
        printPyError()
        raise


def update_timesteps(timesteps, date_start, date_end):

    try:
        updated_timesteps = []
        for timestep in timesteps:

            if(verify_timestep(date_start, date_end, timestep)):
                updated_timesteps.append(timestep)

        return updated_timesteps

    except:
        printPyError()
        raise


def verify_timestep(start, end, timestep):

    valid = False

    try:
        if (start and end):
            if(timestep >= start and timestep <= end):
                valid = True

        elif (start == None and end == None):
            valid = True

        elif (start == None and end):
            if timestep <= date_end:
                valid = True

        elif (start and end == None):
            if timestep >= date_start:
                valid = True

        return valid

    except:
        printPyError()
        raise


def get_pixel_ids(col_names, col_data_types, list_len):
    try:

        pixel_ids_list = [[i] for i in range(1, list_len + 1)]
        if generate_pixel_ids:
            col_names.insert(0, "pixel_id")
            col_data_types.insert(0, "integer")

        pixel_ids_list = np.array(pixel_ids_list)

        return col_names, col_data_types, pixel_ids_list

    except:
        printPyError()
        raise


def create_table(conn, cur, names, data_types):

    #  drop table if it already exists
    q_drop_table = "DROP TABLE IF EXISTS %s.%s;" % (schema, table_name)
    cur.execute(q_drop_table)
    conn.commit()

    column_info_strings = [" ".join([names[i], data_types[i]]) if names[i] != 'time' else " ".join(['"time"', data_types[i]]) for i in range(0, len(names))]
    column_info_strings.insert(0, "id serial PRIMARY KEY")
    q_create_table = ", ".join(column_info_strings)
    q_create_table = "CREATE TABLE %s.%s (%s);" % (schema, table_name, q_create_table)

    cur.execute(q_create_table)
    conn.commit()


def netcdf_get_variables(f, names_only=False):

    nc = Dataset(f, 'r')
    variables = nc.variables

    if names_only:
        return variables.keys()

    else:
        return variables


def netcdf_get_dimensions(f, names_only=False):

    nc = Dataset(f, 'r')
    dimensions = nc.dimensions

    if names_only:
        return dimensions.keys()

    else:
        return dimensions


def netcdf_get_time_values(time_var):
    '''
    time_var: the netCDF4.Variable for time
    '''

    time_vals = []

    if hasattr(time_var, 'units'):
        nc_time_vals = time_var[:].tolist()

        time_units = time_var.units
        start_date_parts = time_units.split(' ')
        period = start_date_parts[0]

        weeks_delta = 0
        days_delta = 0
        hours_delta = 0
        minutes_delta = 0
        seconds_delta = 0
        milliseconds_delta = 0
        microseconds_delta = 0

        ref_date_parts = [int(i) for i in start_date_parts[2].split('-')]
        ref_date = datetime(ref_date_parts[0], ref_date_parts[1], ref_date_parts[2]).date()

        for nc_time_val in nc_time_vals:

            if period == 'weeks':  weeks_delta = nc_time_val
            elif period == 'days':  days_delta = nc_time_val
            elif period == 'hours':  hours_delta = nc_time_val
            elif period == 'minutes':  minutes_delta = nc_time_val
            elif period == 'seconds':  seconds_delta = nc_time_val
            elif period == 'milliseconds':  seconds_delta = nc_time_val
            elif period == 'microseconds':  seconds_delta = nc_time_val

            result = ref_date + timedelta(weeks=weeks_delta, days=days_delta, hours=hours_delta, minutes=minutes_delta, seconds=seconds_delta, milliseconds=milliseconds_delta, microseconds=microseconds_delta)
            time_vals.append(result)

    elif hasattr(time_var, 'long_name'):

        nc_time_vals = time_var[:].tolist()

        for nc_time_val in nc_time_vals:

            start = nc_time_val
            year = int(start)
            rem = start - year
            base = datetime(year, 1, 1)
            result = base + timedelta(seconds=(base.replace(year=base.year + 1) - base).total_seconds() * rem)
            result = result.date()

            time_vals.append(result)

    return time_vals


def netcdf_get_variable_values(var, timestep):

    #  list to hold the values
    values_lst = []

    #  get the raw values from the row
    raw_vals = var[timestep,:, :]

    #  fill blanks with the Null data value
    np.ma.set_fill_value(raw_vals, -9999)
    filled_vals = np.ma.filled(raw_vals)

    shape = raw_vals.shape

    #  add the values to list (from a numpy array)
    for y in range (0, shape[0]):
        the_row = filled_vals[y]
        values_lst.extend(np.ma.filled(the_row.tolist()))

    return values_lst


def get_variables(dims):
    dim_names = dims.keys()
    variables = []
    for f in files:
        temp_variables = netcdf_get_variables(f, names_only=False)
        for temp_variable in temp_variables:
            if not temp_variable in dim_names:
                variables.append(temp_variables[temp_variable])

    return variables


def get_dimension_variables(dims, f):
    dimension_variables = {}
    vars = netcdf_get_variables(f, names_only=False)

    dim_names = dims.keys()
    var_names = vars.keys()

    for v in var_names:
        if v in dim_names:
            dimension_variables[v] = vars[v]

    return dimension_variables


def get_column_data_types(variables):
    return_data_types = ["date", "double precision", "double precision"]

    for variable in variables:
        return_data_types.append("double precision")

    return return_data_types


def get_lons_lats_lists(lons, lats):
    lons_list = []
    lats_list = []
    for lat in lats:
        for lon in lons:
            lons_list.append(lon)
            lats_list.append(lat)
    return lons_list, lats_list


def get_mask_array(longitude_list, latitude_list, shapefiles):

    mask = []

    if (shapefiles is None or shapefiles == []):
        mask = [False] * len(longitude_list)
        return mask

    #  make points
    points = [Point(longitude_list[i], latitude_list[i]) for i in range(0, len(longitude_list))]

    #  union all shapes in the shapefiles
    polygons = []
    for shapefile in shapefiles:
        with fiona.collection(shapefile, "r") as input:
            for feature in input:
                s = shape(feature['geometry'])
                s = s.buffer(1.0)
                polygons.append(s)
    polygon = cascaded_union(polygons)


    for i in range(0, len(longitude_list)):
        if polygon.intersects(points[i]):
            mask.append(False)
        else:
            mask.append(True)

    return mask


def get_variable_value_lists(variables, idx, mask):
    return_list = []

    for var in variables:

        #  get the values for the variable at the specified timestep
        values_list = netcdf_get_variable_values(var, idx)

        #  get masked numpy array
        values_list = np.ma.array(values_list, mask = mask)

        #  get just the valid values as a list
        values_list = np.array(values_list[~values_list.mask]).tolist()

        #  convert the list of values to a list of lists (each value converted to a one-value list)
        values_list = [[str(v)] for v in values_list]
        values_list = np.array(values_list)

        #  add the list of variable value to the output list
        return_list.append(values_list)

    return return_list


def get_stringIO(output_list):
    cpy = cStringIO.StringIO()
    for row in output_list:
        if generate_pixel_ids:
            row_str = str(row[0]) + '\t' + row[1].strftime("%Y-%m-%d %H:%M")+ '\t' + '\t'.join([str(x) for x in row[2:]]) + '\n'
        else:
            row_str = row[0].strftime("%Y-%m-%d %H:%M")+ '\t' + '\t'.join([str(x) for x in row[1:]]) + '\n'
        cpy.write(row_str)
    cpy.seek(0)
    return cpy


def elapsed_time_message(time_start, time_end):
    td = time_end - time_start
    d = td.days
    h, remainder = divmod(td.seconds, 3600)
    m, s = divmod(remainder, 60)
    s += td.microseconds / 1e6
    print "Elapsed time: %d:%d:%d:%s" % (d, h, m, s)


#-----Main Code------------------------------------------------------------------------------------

def main():
    try:

        time_start = datetime.now()

        #  connection info
        conn = None
        curs = None
        conn_string = None

        if password == "":
            conn_string = "host=%s dbname=%s port=%s user=%s" % (host, dbname, port, user)
        else:
            conn_string = "host=%s dbname=%s port=%s user=%s password=%s" % (host, dbname, port, user, password)

        #  verify date range
        verifyDateRange(date_start, date_end)

        #  list to keep track of the column names
        column_names = []
        column_names.append("time")
        column_names.append("lon")
        column_names.append("lat")

        #  get the dimensions from one of the NetCDF file
        dimensions = netcdf_get_dimensions(files[0], names_only=False)
        dimension_variables = get_dimension_variables(dimensions, files[0])

        #  get variables that are not dimensions
        variables = get_variables(dimensions)

        #  add the variable name to the list of columns
        for variable in variables:
            column_names.append(variable.name)

        #  get the column datatypes
        column_data_types = get_column_data_types(variables)

        timesteps = netcdf_get_time_values(dimension_variables["time"])
        timesteps = update_timesteps(timesteps, date_start, date_end)

        lons_list = dimension_variables["lon"][:].tolist()
        lats_list = dimension_variables["lat"][:].tolist()

        #  get list length considering all possible long, lat values
        list_length_for_pixel_ids = len(lons_list) * len(lats_list)

        column_names, column_data_types, pixel_ids_list = get_pixel_ids(column_names, column_data_types, list_length_for_pixel_ids)

        lons_list, lats_list = get_lons_lats_lists(lons_list, lats_list)

        mask = get_mask_array(lons_list, lats_list, shapefile_paths)

        lons_list = np.ma.array(lons_list, mask = mask)
        lats_list = np.ma.array(lats_list, mask = mask)
        pixel_ids_list = np.ma.array(pixel_ids_list, mask = mask)

        lons_list = np.array(lons_list[~lons_list.mask])
        lats_list = np.array(lats_list[~lats_list.mask])
        pixel_ids_list = np.array(pixel_ids_list[~pixel_ids_list.mask])

        lons_list = lons_list.tolist()
        lats_list = lats_list.tolist()
        pixel_ids_list = pixel_ids_list.tolist()

        lons_list = [[l] for l in lons_list]
        lats_list = [[l] for l in lats_list]
        pixel_ids_list = [[l] for l in pixel_ids_list]

        lons_list = np.array(lons_list)
        lats_list = np.array(lats_list)
        pixel_ids_list = np.array(pixel_ids_list)

        list_length = mask.count(False)

        #  connect to the database
        conn = psycopg2.connect(conn_string)
        curs = conn.cursor()

        #  create the table in the database
        create_table(conn, curs, column_names, column_data_types)

        #  loop over the timesteps
        timestep_idx = -1
        for timestep in timesteps:

            timestep_idx += 1
            output_list = []

            print "Processing: timestep %d of %d" % (timestep_idx + 1, len(timesteps))

            #  list for the current timestep
            time_list = [[timestep]] * list_length
            time_list = np.array(time_list)

            #  add time, lon, and lat values to the list
            if generate_pixel_ids:
                output_list.append(pixel_ids_list)
            output_list.extend([time_list, lons_list, lats_list])

            #  get variable value lists
            values_lists = get_variable_value_lists(variables, timestep_idx, mask)

            #  extend output_list with the lists of value lists
            output_list.extend(values_lists)

            #  stack the values
            output_list = np.hstack(np.array(output_list))

            cpy = get_stringIO(output_list)

            #  write data for the timestep to the database
            curs.copy_from(cpy, schema + "." + table_name, columns=column_names)
            conn.commit()

    except psycopg2.Error, e:
        print_psycopg_error(e)

    except:
        printPyError()

    finally:
        if not curs is None:
            curs.close()
        if not conn is None:
            conn.close()

        time_end = datetime.now()
        elapsed_time_message(time_start, time_end)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--files", nargs='+', help="List of NetCDF paths", required=True)
    parser.add_argument("-o", "--host", help="host", type=str, required=True)
    parser.add_argument("-r", "--port", help="port", type=str, required=False)
    parser.add_argument("-d", "--database", help="database", type=str, required=True)
    parser.add_argument("-u", "--username", help="username", type=str, required=False)
    parser.add_argument("-p", "--password", help="password", type=str, required=False)
    parser.add_argument("-s", "--schema", help="schema", type=str, required=True)
    parser.add_argument("-t", "--table", help="table", type=str, required=True)
    parser.add_argument("-i", "--ids", help="generate pixel ids", type=str, required=False)
    parser.add_argument("-a", "--start", help="start date formatted as 'm,d,yyyy'",type=str,  required=False)
    parser.add_argument("-b", "--end", help="end date formatted as 'm,d,yyyy'", type=str, required=False)
    parser.add_argument("-g", "--shapefiles", nargs='+', help="list of shapefiles", required=False)

    args = parser.parse_args()

    files = args.files
    host = args.host
    dbname = args.database
    port = "" if args.port is None else args.port
    user = "" if args.username is None else args.username
    password = "" if args.password is None else args.password
    schema = args.schema
    table_name = args.table
    generate_pixel_ids = True if args.ids is None else args.ids
    date_start = args.start
    date_end = args.end
    shapefile_paths = args.shapefiles

    print
    print "files:", files
    print "host:", host
    print "database:", dbname
    print "port:", port
    print "user:", user
    print "password:", password
    print "schema:", schema
    print "table:", table_name
    print "pixel ids:", generate_pixel_ids
    print "start date:", date_start
    print "end date:", date_end
    print "shapefiles:", shapefile_paths
    print

    main()