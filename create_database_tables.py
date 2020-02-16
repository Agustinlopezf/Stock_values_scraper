import psycopg2
import json

def initialize():
    #Read the config file and obtain the database credentials and tables settings
    with open('config.json', 'r') as config_file:
        json_file = json.load(config_file) #As the load method takes a file as input. Use f.read() when using json.loads()
    credentials = json_file['credentials']
    tables_dict = json_file['tables_data']

    connection(credentials, tables_dict)

def connection(credentials, tables_dict):
    #Connect to the database and create cursor
    try:

        connection = psycopg2.connect(user = credentials['user_credentials']['user'], 
            password = credentials['user_credentials']['password'], database = credentials['database_name'])
        
        cursor = connection.cursor()
        print(connection.get_dsn_parameters(), '\n')

        # Print PostgreSQL version
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("You are connected to - ", record,"\n")

        for table_name in tables_dict.keys():
            create_table(cursor, table_name, tables_dict[table_name]['table_variables'])
            #Commit changes to the database
            connection.commit()


    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL", error)
    finally:
        #closing database connection.
            if(connection):
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")


def create_table(cursor, table_name, fields_dict):
    #The function takes a cursor, the table name and a dict containing the table variable names as key and the type as the value
    #The execution string is created and then executed
    
    #Delete the table if it already exists
    cursor.execute('DROP TABLE IF EXISTS {table_name};'.format(table_name = table_name))

    execution_string = 'CREATE TABLE ' + table_name + ' (ID SERIAL PRIMARY KEY, '
    fields_string = ', '.join(['{k} {val}'.format(k = key, val = val) for key, val in fields_dict.items()])
    execution_string = execution_string + fields_string + ');'
    cursor.execute(execution_string)
    print('Table ' + table_name + ' created')

if __name__ == '__main__':
    initialize()