
m functools import partial
from threading import local
from osgeo import ogr
import pandas as pd
import geopandas as gpd
import os
from sqlalchemy import create_engine, schema, inspect, exc # fix this
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker, scoped_session, Session
from dotenv import load_dotenv
from os.path import join, dirname
import glob
import subprocess
import fileinput
from datetime import datetime
import docker
import sqlparse
import re

from sqlalchemy import text

#Runs the docker client (essential)
client = docker.from_env()

def docker_client():
    return client.containers.get('lm-db')

# Gets timestamp
def timestamp(time=False, date=False):
    now = datetime.now()
    if time:
        return now.strftime("%d%m%y")
    if date:
        return now.strftime("%H%M")

# Loads .env file
def load_env(local=False, dev1=False, docker=False):
    dotenv_path = join(dirname(__file__), os.path.expanduser(f"~/lm/.env")) # .env lives in the library's root directory
    load_dotenv(dotenv_path)

    if local:
        user=os.getenv('LOCAL_USERNAME')
        password=os.getenv('LOCAL_PASSWORD')
        host=os.getenv('LOCAL_HOST')
        port=os.getenv('LOCAL_PORT')
        database=os.getenv('LOCAL_DATABASE')
        return user, password, host, port, database

    if dev1:
        user=os.getenv('DEV1_USERNAME')
        password=os.getenv('DEV1_PASSWORD')
        host=os.getenv('DEV1_HOST')
        port=os.getenv('DEV1_PORT')
        database=os.getenv('DEV1_DATABASE')
        return user, password, host, port, database

    if docker:
        user=os.getenv('DOCKER_USERNAME')
        password=os.getenv('DOCKER_PASSWORD')
        host=os.getenv('DOCKER_HOST')
        port=os.getenv('DOCKER_PORT')
        database=os.getenv('DOCKER_DATABASE')
        return user, password, host, port, database

def make_dump(local=False, dev1=False, docker=False, dot_dump=False, dot_sql=False, table=False, schemas=False, adfi=False, restore=False, new_database=False):
    
    if local:
        user, password, host, port, database = load_env(local=True)

    if dev1:
        user, password, host, port, database = load_env(dev1=True)

    if docker:
        user, password, host, port, database = load_env(docker=True)

    partial_path = os.path.expanduser(f"/var/lib/postgresql/data/lm_data/")

    if dot_dump:
        file_path = f'whole_{database}_' + 'local_' + timestamp(time=True) + '_' + timestamp(date=True) + '.dump'
        directory = partial_path + file_path
        specifics = '-Fc'

        print(f'Making dump: {file_path}')
        print('')

    if dot_sql:
        file_path = f'whole_{database}_' + 'local_' + timestamp(time=True) + '_' + timestamp(date=True) + '.sql'
        directory = partial_path + file_path
        specifics = ''

        print(f'Making dump: {file_path}')
        print('')

    if table:
        answer = input('Table to dump: ')
        file_path = f'specific_' + answer.replace('.', '__') + '_local_' + timestamp(time=True) + '_' + timestamp(date=True) + '.sql'
        directory = partial_path + file_path
        specifics = "--table=" + "'" + answer.replace(".", "'.'") + "'"

        print(f'Making dump: {file_path}')
        print('')

    if schemas:
        answer = input('Schemas (separated by commas): ')

        specifics = ''
        count = 0

        for schema in answer.replace(' ', '').split(','):
            count += 1
            flag = "-n "
            parameter = flag + schema

            if count == 1:
                specifics += parameter

            if count > 1:
                specifics += ' ' + parameter

        file_path = f'specific_' + answer.replace(', ', '__') + '_local_' + timestamp(time=True) + '_' + timestamp(date=True) + '.sql'
        directory = partial_path + file_path
        print(f'Creating {file_path}')
        print('')

    if adfi:
        answer = input('Table to dump: ')
        file_path = f'specific_' + answer.replace('.', '__') + '_local_' + timestamp(time=True) + '_' + timestamp(date=True) + '.sql'
        directory = partial_path + file_path
        specifics = "--table=" + "'" + answer.replace(".", "'.'") + "'"

        print(f'Creating {file_path}')
        print('')

    postgis = docker_client()

    command = f"sh -c 'pg_dump --dbname=postgresql://{user}:{password}@{host}:{port}/{database} {specifics} > {directory}'"
    # command = f"sh -c 'pg_dump --dbname=postgresql://lm:password@localhost:5432/livingmap_db {specifics} > {directory}'"
    log = postgis.exec_run(command, stdout=True, stderr=True)
    print("Message from Docker:")
    print(log)
    print('__________________')

    if adfi:
        return file_path, answer

    if restore and new_database:
        with engine_creation(local=True).connect() as conn:
            print('moved into creating engine')
            print(conn, new_database)
            conn.execute("commit")
            conn.execute(f"CREATE DATABASE {new_database}")
            print('Database Created.')

        session = Session()
        engine = sa.create_engine(f'postgresql://lm:password@localhost:5432/{new_database}')
        engine.execute("CREATE EXTENSION postgis;")
        print('PostGIS extension created.')
        print('moved into restoring process')
        command = f"sh -c 'pg_restore --dbname=postgresql://lm:password@localhost:5432/{new_database} --no-owner --no-privileges {directory}'"
        log = postgis.exec_run(command, stdout=True, stderr=True, environment=["PASSWORD=password"])
        print('Restoring database...')
        print('')
        print("Message from Docker:")
        print(log)
        print('__________________')

def engine_creation(local=False, dev1=False, docker=False):
    if local:
        user, password, host, port, database = load_env(local=True)
        return create_engine("postgresql://{}:{}@{}:{}/{}".format(user, password, host, port, database))
    if dev1:
        user, password, host, port, database = load_env(dev1=True)
        return create_engine("postgresql://{}:{}@{}:{}/{}".format(user, password, host, port, database))
    if docker:
        user, password, host, port, database = load_env(docker=True)
        return create_engine("postgresql://{}:{}@{}:{}/{}".format(user, password, host, port, database))

def data_validation():

    def table_exists(engine,name, schema):
        ins = inspect(engine)
        ret =ins.dialect.has_table(engine.connect(),name, schema=schema)
        return ret

    table_name = input('Name of table: ')
    main_geo_df = gpd.GeoDataFrame()

    rootdir = input('Select directory to validate: ')

    print('1. Running geometry validation checks...')
    for subdir, dirs, files in os.walk(rootdir):
        for file in files:
            if '.geojson' in file:
                path = os.path.join(subdir, file)
                single_geo_df = gpd.read_file(path)
                single_geo_df['file'] = file
                single_geo_df['is_valid'] = single_geo_df.geometry.is_valid
                main_geo_df = main_geo_df.append(single_geo_df)
                main_geo_df.reset_index(drop=True, inplace=True)

            else:
                continue

    if main_geo_df['is_valid'].all() == False:
        print('2. Validation check: Invalid geometry/geometries found. Please red log.')
        #Final location can be variabalise with name of table_name
        main_geo_df[main_geo_df['is_valid'] == False].to_csv(rootdir+'/invalid_{}_geom.csv'.format(table_name))
    else:
        print('2. Validation check: No invalid geometry/geometries found.')
        main_geo_df.drop(columns=['is_valid'], inplace=True)
        engine = create_engine("postgresql://{}:{}@{}:{}/{}".format(user, password, host, port, database))

        schema_input = input('Schema name: ')
        try:
            if not engine.dialect.has_schema(engine, schema_input):
                engine.execute(schema.CreateSchema(schema_input))
                print('3. "{}" schema created.'.format(schema_input))

                main_geo_df.to_postgis(table_name, engine, schema=schema_input)
                print('4. "{}" table exported to "{}" schema.'.format(table_name, schema_input))

            elif engine.dialect.has_schema(engine, schema_input):
                answer = input('The schema "{}" exists, would you still like to insert table? (yes or no): '.format(schema_input))
                if answer == 'yes':
                    if table_exists(engine, table_name, schema_input) == True:
                        table_name = input('"{}" table already exists. Please write a different table name (or type "cancel" to exit): '.format(table_name))
                        if table_name != 'cancel':
                            main_geo_df.to_postgis(table_name, engine, schema=schema_input)
                            print('3. "{}" exported to "{}" schema.'.format(table_name, schema_input))
                        else:
                            print('Process aborted.')
                            exit()

                    else:
                        main_geo_df.to_postgis(table_name, engine, schema=schema_input)
                        print('3. "{}" exported to "{}" schema.'.format(table_name, schema_input))
                        pass

                elif answer == 'no':
                    print('Process aborted.')
                    pass

                elif (answer != 'yes') | (answer != 'no'):
                    print('Error. Please try again.')
                    pass

            else:
                print('Error. Unable to proceed.')
                pass

        except exc.ProgrammingError:
            pass

def map_onboard():

    def missing_zeros(text):
        missing_zeros = 4 - len(text)
        if missing_zeros == 0:
            pass
        else:
            return '0' * missing_zeros + text

    print('Map On-Board Process Started...')

    engine = engine_creation(local=True)

    sql = sa.text('SELECT version FROM beauford_house_gis.flyway_schema_history')
    highest_gis_schema_version = str(max([int(i[0]) for i in engine.execute(sql) if i[0] is not None]))
    highest_gis_schema_version = missing_zeros(highest_gis_schema_version)
    
    sql = sa.text('SELECT version FROM gis_universal_tables.flyway_schema_history')
    highest_universal_schema_data_version = str(max([int(i[0]) for i in engine.execute(sql) if i[0] is not None]) + 1)
    highest_universal_schema_data_version = missing_zeros(highest_universal_schema_data_version)

    sql = sa.text('SELECT version FROM beauford_house_views.flyway_view_history')
    highest_views_version = str(max([int(i[0]) for i in engine.execute(sql) if i[0] is not None]))
    highest_views_version = missing_zeros(highest_views_version)

    project_name = input('Project name: ')
    docker_container_name = input('Docker container name: ')

    ### Create the manual project creation – /manual_project_creation/
    file_path = os.path.expanduser(f"~/Documents/database-management/services/mapping/manual_project_creation/{project_name}_creation_script_@_V{highest_gis_schema_version}_gis_V{highest_views_version}_views.sql")

    def dump_schema(host, port, database, user, password, gis_schema_version, views_version):
        command = f'docker exec -i {docker_container_name} ' \
            f'pg_dump ' \
            f'--no-privileges ' \
            f'--no-owner ' \
            f'-s ' \
            f'-n beauford_house_gis -n beauford_house_mms -n beauford_house_views ' \
            f'--dbname=postgresql://{user}:{password}@{host}:{port}/{database} ' \
            f'> {file_path}'

        proc = subprocess.Popen(command, shell=True, env={**os.environ, "PGPASSWORD": password})
        proc.wait()
    
    user, password, host, port, database = load_env(docker=True)
    dump_schema(host, port, database, user, password, highest_gis_schema_version, highest_views_version)

    with fileinput.FileInput(file_path, inplace=True) as file:
        for line in file:
            print(line.replace('beauford_house', project_name), end='')
    
    ### Create a versioned project data baseline – /versioned_project_data/
    
    number_of_floors = int(input('How many floors has the project? '))
    
    floors_name = '-- floors_name'+'\n'
    for floor in range(0, number_of_floors):
        floors_name += f"        WHEN floor = {floor} THEN 'Floor {floor}'" + '\n'
    floors_short_name = '-- floors_short_name'+'\n'
    for floor in range(0, number_of_floors):
        floors_short_name += f"        WHEN floor = {floor} THEN '{floor}'" + '\n'

    V0001__baseline_sql = open(os.path.dirname(os.path.abspath(__file__))+'/templates/V0001__baseline.sql').read().replace('project_name', project_name).replace('list_of_floors', ", ".join(str(_) for _ in range(0, number_of_floors))).replace('floors_name', floors_name).replace('floors_short_name', floors_short_name)

    mode = 'w+'
    filename = 'V0001__baseline.sql'
    V0001__baseline_path = os.path.expanduser(f"~/Documents/database-management/services/mapping/versioned_project_data/{project_name}/")
    os.makedirs(V0001__baseline_path, exist_ok=True)
    with open(V0001__baseline_path+filename, mode) as f:
        f.write(V0001__baseline_sql)

    ### Create a project config within GIS Universal Tables – /versioned_universal_schema_data/schema/
    add_config_template = open(os.path.dirname(os.path.abspath(__file__))+'/templates/add_config.sql').read().replace('project_name', project_name)

    versioned_universal_schema_data__schema__path = os.path.expanduser(f"~/Documents/database-management/services/mapping/versioned_universal_schema_data/schema/")
    highest_versioned_universal_schema_data__schema = max([int(file.replace(versioned_universal_schema_data__schema__path, '').replace('V', '').split('__')[0]) for file in glob.glob(versioned_universal_schema_data__schema__path+'*')])
    new_highest_versioned_universal_schema_data__schema = highest_versioned_universal_schema_data__schema + 1
    new_highest_versioned_universal_schema_data__schema = str(new_highest_versioned_universal_schema_data__schema)
    new_highest_versioned_universal_schema_data__schema = missing_zeros(new_highest_versioned_universal_schema_data__schema)

    mode = 'w+'
    filename = 'V' + new_highest_versioned_universal_schema_data__schema + f'__add_config_{project_name}.sql'
    add_config_path = os.path.expanduser(f"~/Documents/database-management/services/mapping/versioned_universal_schema_data/schema/")

    with open(add_config_path+filename, mode) as f:
        f.write(add_config_template)

    ### Update views to include the new project variable – /versioned_universal_schema_data/views/
    versioned_universal_schema_data__views__path = os.path.expanduser(f"~/Documents/database-management/services/mapping/versioned_universal_schema_data/views/")
    highest_versioned_universal_schema_data__views = max([int(file.replace(versioned_universal_schema_data__views__path, '').replace('V', '').split('__')[0]) for file in glob.glob(versioned_universal_schema_data__views__path+'*')])
    highest_versioned_universal_schema_data__views_str = str(highest_versioned_universal_schema_data__views)
    highest_versioned_universal_schema_data__views_str = missing_zeros(highest_versioned_universal_schema_data__views_str)
    highest_versioned_universal_schema_data__views_file_path = [file for file in glob.glob(versioned_universal_schema_data__views__path+'*') if highest_versioned_universal_schema_data__views_str in file][0]
    highest_versioned_universal_schema_data__views_file = open(highest_versioned_universal_schema_data__views_file_path).read()
    new_highest_versioned_universal_schema_data__views_file = highest_versioned_universal_schema_data__views_file.replace('-- automation_flyway_dashboard', open(os.path.dirname(os.path.abspath(__file__))+'/templates/flyway_dashboard.sql').read().replace('project_name', project_name))
    new_highest_versioned_universal_schema_data__views = highest_versioned_universal_schema_data__views + 1
    new_highest_versioned_universal_schema_data__views_str = str(new_highest_versioned_universal_schema_data__views)
    new_highest_versioned_universal_schema_data__views_str = missing_zeros(new_highest_versioned_universal_schema_data__views_str)

    mode = 'w+'
    filename = 'V' + new_highest_versioned_universal_schema_data__views_str + f'__flyway_dashboard_{project_name}.sql'
    flyway_dashboard_path = os.path.expanduser(f"~/Documents/database-management/services/mapping/versioned_universal_schema_data/views/")

    with open(flyway_dashboard_path+filename, mode) as f:
        f.write(new_highest_versioned_universal_schema_data__views_file)

    print('Success! Map On-Board Process Finished.')

def dumps():
    
    whole_or_specific = int(input('Type 1 (whole database), type 2 (specific): '))
    if whole_or_specific == 1:
        dump_or_sql = int(input('Type 1 (.dump), type 2 (.sql): '))
        if dump_or_sql == 1:
            restore_boolean = input('Restore? True (i.e. Yes, restore) or False (i.e. No, don\'t restore)? ')
            restore_boolean = restore_boolean.lower()
            if restore_boolean == 'true':
                database = input('New database name: ')
                restore_boolean = True
                make_dump(docker=True, dot_dump=True, restore=restore_boolean, new_database=database)
            
            if restore_boolean == 'false':
                restore_boolean = False
                make_dump(docker=True, dot_dump=True, restore=restore_boolean)

        if dump_or_sql == 2:
            make_dump(docker=True, dot_sql=True)

    if whole_or_specific == 2:
        single_table_or_single_schema_or_schemas = int(input('Type 1 for single table, type 2 for schema(s): '))
        if single_table_or_single_schema_or_schemas == 1:
            make_dump(docker=True, table=True)
 
        if single_table_or_single_schema_or_schemas == 2:
            make_dump(docker=True, schemas=True) 

    print('Process ended successfully.')

def dump_to_flyway():

    file_name_sql, answer = make_dump(docker=True, adfi=True)

    full_dump_path = os.path.expanduser(f"~/Documents/dumps/{file_name_sql}")
    dump_file = open(full_dump_path).read()
    dump_file = dump_file.replace('all_data_for_import', 'imports')
    no_comments_dump_file = sqlparse.format(dump_file,  strip_comments=True).strip()
    statements = sqlparse.split(no_comments_dump_file)

    try:
        schema = re.search('CREATE TABLE (.+?).imports ', no_comments_dump_file).group(1)
    except AttributeError:
        print('No schema found using Regex.')

    mode = 'w+'
    file_name_sql = f'edited_' + answer.replace('.', '__') + '_local_' + timestamp(time=True) + '_' + timestamp(date=True) + '.sql'
    full_edited_dump_path = os.path.expanduser(f"~/Documents/dumps/dumps-mods/all_data_for_import/{file_name_sql}")
    with open(full_edited_dump_path, mode) as f:
        create_schema_statement = f'CREATE SCHEMA {schema};'
        sql_text = create_schema_statement + '\n\n' +  statements[12] + '\n\n' + statements[14] +'\n' + statements[15]
        f.write(sql_text)

    print('Process ended successfully.')

################################################################
## FUTURE IMPLEMENTATIONS ######################################
################################################################

# def MD5(geom, name, category, _class, type, floor_level):
#     if 
#     geom = str(geom)
#     name = str(name)
#     category = str(category)
#     _class = str(_class)
#     type = str(type)
#     floor_level = str(floor_level)

# from hashlib import md5 as MD5
# print(md5("whatever your string is".encode('utf-8')).hexdigest())

# MD5(concat(geom, name, category, class, type, floor_level))

# MD5(concat(geom, name, category, class, type, floor_level))
# from shapely import geos, wkb, wkt
# geom = wkb.dumps(df.geom[0], hex=True, srid=3857)
# geom
# name = df.name[0]
# name
# category = df.category[0]
# category
# xclass = df['class'][0]
# xclass
# xtype = df['type'][0]
# xtype
# floor_level = str(df['floor_level'][0])
# floor_level
# python = geom + name + category + xclass + xtype
# from hashlib import md5 as MD5
# python_MD5 = MD5(python.encode('utf-8')).hexdigest()
# postgis_MD5 = df.lm_id[0]
# print(python_MD5 == postgis_MD5)
# def MD5(geom, name, category, class, type, floor_level):
#     print('e')
# if None not in (geom, name, category, xclass, xtype, floor_level):
#     pass 
