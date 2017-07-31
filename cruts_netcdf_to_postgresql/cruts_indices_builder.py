# -*- coding: utf-8 -*-
"""
Created on Fri Oct  7 13:47:48 2016
This script is to be run after the cruts climate data has be in ingested into a database.

Parameter examples
h = 'terrapop-internal-db.pop.umn.edu'
db = 'terrapop_v2_development_asoni'
p = 5432
u = 'dahaynes'
cruts_table_name = 'climate.cruts'
@author: David Haynes
"""

import psycopg2
import argparse
import sys



def check_table_exists(cur, cruts_table):
    '''Query PostgreSQL to identify  '''
    t_schema, t_name = cruts_table.split(".")
    query = "select table_name from information_schema.tables where table_schema = '%s' and table_name = '%s';" % (t_schema, t_name)

    cur.execute(query)
    results = cur.fetchone()
    if results == None:
        return 0
    else:
        return 1


def gettablecolumns(cur, cruts_table):
    '''Query PostgreSQL to identify  '''
    t_schema, t_name = cruts_table.split(".")
    query = "select column_name from information_schema.columns where table_schema = '%s' and table_name = '%s';" % (t_schema, t_name)

    cur.execute(query)
    results = cur.fetchall()
    return results

def parsecrutscolumns(columns):
    '''Remove the extra columns found in cruts. Return only the cruts variables '''
    crutscolumns = ['id', 'pixel_id', 'time', 'lon', 'lat']
    
    del_vars = [c for var in crutscolumns for c in columns if var == c[0]]
    for i in del_vars:
        columns.remove(i)
    
    myvars = [i[0] for i in columns]  
    return myvars

def create_cruts_all_variables_templates(cur, cruts_table, cruts_variables):
    '''Create a cruts climate template for all cruts variables '''
    t_schema, t_table = cruts_table.split(".")

    for variable in cruts_variables:
        #variable = i[0]
        query = "select distinct pixel_id into %s.%s_%s from %s where %s = -9999" % (t_schema, t_table, variable, cruts_table, variable)
        #print query
        cur.execute(query)
        

def create_cruts_two_variable_templates(cur, cruts_table, cruts_variables):
    '''This function supercedes the create_cruts_all_variables_templates. This will create two templates that are appropriate for all cruts climate variables '''
    anomaly = 'pet'
    
    t_schema, t_table = cruts_table.split(".")


    
    if len(cruts_variables) == 1:
        if anomaly in cruts_variables:
            variabledict = {anomaly: '%s_template' % (anomaly)}
        else:
            variabledict = {cruts_variables[0]: 'all_template'}
    else:        
        variabledict = {}
        if anomaly in cruts_variables:
            variabledict[anomaly] ='%s_template' % (anomaly)
            cruts_variables.remove(anomaly)
            
        variabledict[cruts_variables[0]] = 'all_template'
    
    
    for v in variabledict:
        #This is the drop query
        query = 'drop table if exists %s.%s_%s;' % (t_schema, t_table, variabledict[v])
        #print query
        cur.execute(query)
        
        query = '''with pixel_template as
                (
                select distinct pixel_id, lon, lat 
                from %s 
                where %s != -9999
                )
                Select pixel_id, ST_GEOMFROMTEXt( 'POINT (' || lon || ' ' || lat || ')', 4326) as geom 
                into %s.%s_%s
                from pixel_template ''' % (cruts_table, v, t_schema, t_table, variabledict[v])

        #query = "select distinct pixel_id into %s.%s_%s from %s where %s = -9999" % (t_schema, t_table, variabledict[v], cruts_table, v)
        #print query
        cur.execute(query)
        
        query  = "create index %s_%s_%s_pixel_id on %s.%s_%s using btree(pixel_id);" % (t_schema, t_table, variabledict[v], t_schema, t_table, variabledict[v])
        #print query
        cur.execute(query)
        
        
def create_cruts_time_indices(cur, cruts_table):
    '''These indices take about 15 minutes each to create '''
    
    queries= []    
    cruts = cruts_table.replace('.', '_')
    
    query  = "create index %s_month on %s using btree(date_part('month', time));" % (cruts, cruts_table)
    queries.append(query)
    
    query  = "create index %s_year on %s using btree(date_part('year', time));" % (cruts, cruts_table)
    queries.append(query) 
    
    query  = "create index %s_date on %s using btree(time);" % (cruts, cruts_table)
    queries.append(query)
    
    query  = "create index %s_pixel_id on %s using btree(pixel_id);" % (cruts, cruts_table)
    queries.append(query)
    
    query  = "create index %s_month_year on %s using btree( date_part('year', time), date_part('month', time) );" % (cruts, cruts_table)
    queries.append(query)
    
    for i in queries:
        #print i
        cur.execute(i)
        

def create_cruts_variable_indices(cur, cruts_table, variables):
    '''This function will create indices for every cruts variable. These indices take about 15 minutes each to create '''
    cruts = cruts_table.replace('.', '_')
    
    for i in variables:
        query  = "create index %s_%s on %s using btree(%s);" % (cruts, i, cruts_table, i)
        #print query
        cur.execute(query)
         

def create_cruts_countries_table(cur, cruts_table):
    '''This function will create the countries table '''
    query = "DROP TABLE IF EXISTS %s_countries; " % (cruts_table)
    cur.execute(query)
    
    query = "create table %s_countries (id bigint, country_id bigint, country_name text, iso_code text, cruts_all_template text, cruts_pet_template text);" % (cruts_table)
    #print query    
    cur.execute(query)


def main(argv):
    '''Main Function'''

    parser = argparse.ArgumentParser()
    parser = argparse.ArgumentParser(description = "Adds Indices to the loaded cruts table")
    parser.add_argument("-o", "--host", help="host", type=str, required=True)
    parser.add_argument("-r", "--port", help="port", type=str, required=False)
    parser.add_argument("-d", "--database", help="database", type=str, required=True)
    parser.add_argument("-u", "--username", help="username", type=str, required=False)
    parser.add_argument("-p", "--password", help="password", type=str, required=False)
    parser.add_argument("-s", "--schema", help="schema", type=str, required=True)
    parser.add_argument("-t", "--table", help="table", type=str, required=True)

   
    args = parser.parse_args()
    
    db = args.database
    p = args.port
    s = args.host
    u = args.username

    cruts_table_name = '%s.%s' % (args.schema, args.table)
    
    print "host:", s
    print "database:", db
    print "port:", p
    print "user:", u
    print "schema:", args.schema
    print "table:", args.table
    print "PostGIS Table:", cruts_table_name       
    #print env,db
    if db and p and s and cruts_table_name:
        try:
            connection = psycopg2.connect(host=s, database=db, port=p, user=u)
            if connection:
                cursor = connection.cursor()
    
                table_exists = check_table_exists(cursor, cruts_table_name)
    
                if table_exists:
                    allcolumns = gettablecolumns(cursor, cruts_table_name)
                    allvariables = parsecrutscolumns(allcolumns)
                    print "All climate variables found in %s: %s" % (cruts_table_name, allvariables)
    
                    #Create the temporal indices
                    #create_cruts_time_indices(cursor, cruts_table_name)
                    #Create variable_indices
                    #create_cruts_variable_indices(cursor, cruts_table_name, allvariables)
    
                    #Create cruts template tables.
                    create_cruts_two_variable_templates(cursor, cruts_table_name, allvariables)
    
                    #Create cruts_all variables templates
                    #This function is commented out as we only need 2 templates
                    #create_cruts_all_variables_templates(cursor, cruts_table_name, allvariables)
    
                    #Create the cruts_vXX countries table
                    #create_cruts_countries_table(cursor, cruts_table_name)
                    connection.commit()
                    
                    #Close things up
                    cursor.close()
                    connection.close()
                    
                else:
                    print "Connection created. Specified table not found in database"
                    sys.exit()
                
                                

        except psycopg2.Error, e:
            print "No connection made:", e
            sys.exit()
            #connection.close()
                
    
    else:
        print "Error required parameter port, host, db, or table name not set"
        sys.exit()


if __name__ == "__main__":
    main(sys.argv[1:])



