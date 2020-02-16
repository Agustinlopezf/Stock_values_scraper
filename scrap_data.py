from bs4 import BeautifulSoup
import requests
import re
import psycopg2
from datetime import date, datetime
import json


def initialize():
    #Read the config file and obtain the database credentials and tables settings
    with open('config.json', 'r') as config_file:
        json_file = json.load(config_file)
    credentials = json_file['credentials']
    tables_dict = json_file['tables_data']

    #Extract a dict of all soups
    soup_dict = obtain_soup(tables_dict)
    
    #Obtain the scrapped data
    scrapped_data = extract_data(soup_dict, tables_dict)

    #Save data to database
    save_to_database(scrapped_data, credentials, tables_dict)

def obtain_soup(tables_dict):
    soup_dict = {}
    for key, item in tables_dict.items():
        website_url = item['url']
        page = requests.get(website_url)
        soup_dict[key] = BeautifulSoup(page.content, 'html.parser')
    return soup_dict

def extract_data(soup_dict, tables_dict):
    scrapped_data = {key: [] for key in soup_dict.keys()} #The dict contains a list of rows for each key, which will be filled with the scrapped data
    for key in soup_dict.keys():
        if key == 'DOLAR': #As the dolar table has a different structure
            list_rows = soup_dict[key].find_all('td', string = re.compile(r'^DOLAR'))
            for item in list_rows:
                row_list = [format_value(x.text) for x in item.parent.find_all('td')]
                row_list.insert(0, "'{}'".format(str(date.today()))) #Append date to the beginning of the list
                scrapped_data[key].append(row_list)
        else:
            list_rows = soup_dict[key].find_all('a', attrs={'href': re.compile(r'^/empresas/perfil')})
            for row in list_rows:
                row_list = []
                if key == 'INDICADORES':
                    elem_list = row.parent.parent.parent.parent.find_all('td') #The structure is different from the another tables
                    #As the Future value will not be saved
                    del elem_list[8]
                    del elem_list[12]
                else:
                    elem_list = row.parent.parent.find_all('td')
                    if key == 'ADR':
                        #For ADR the last two elements are not saved
                        del elem_list[8]
                        del elem_list[7]
                for html_element in elem_list:
                    row_list.append(format_value(html_element.text))
                row_list.insert(0, "'{}'".format(str(date.today()))) #Append date to the beginning of the list
                scrapped_data[key].append(row_list)
    return scrapped_data

def format_value(value):
    #Format the value to change number from spanish to english, and add single quotes so it can be saved in PostgreSQL
    if value == '-' or value == '':
        return 'NULL' #As the NULL value does not require extra single quotes
    elif re.match(r'[0-9]{2}:[0-9]{2}', value): 
        #If the value is an time with format XX:XX, add seconds
        value = value + ":00"
    elif re.match(r'[0-9]{2}/[0-9]{2}/[0-9]{2}', value): 
        #If the value is a date with format dd/mm/yy, format it to english date format to save in the database
        value = datetime.strptime(value, "%d/%m/%y").date()
    else:
        value = value.replace('.', '').replace(',', '.')
    return "'{}'".format(value) #Format value in single quotes to save to PostgreSQL

def save_to_database(scrapped_data, credentials, tables_dict):
    try:

        connection = psycopg2.connect(user = credentials['user_credentials']['user'], 
            password = credentials['user_credentials']['password'], database = credentials['database_name'])
        
        cursor = connection.cursor()
        print(connection.get_dsn_parameters(), '\n')

        # Print PostgreSQL version
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("You are connected to - ", record,"\n")

        for table_name in scrapped_data.keys():
            variables_format_string = ' ({})'.format(', '.join(tables_dict[table_name]['table_variables'].keys()))
            for row in scrapped_data[table_name]:
                variables_value_string = '({});'.format(', '.join(row))
                execution_string = 'INSERT INTO ' + table_name + variables_format_string + ' VALUES ' + variables_value_string
                cursor.execute(execution_string)
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

if __name__ == '__main__':
    initialize()
