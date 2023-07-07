import psycopg2
from pymongo import MongoClient

# A dictionary to make  the table_postgres function run more smoothly
dataset = {'visitors': {'_id': 'varchar(500)', 'recommendable': 'varchar(500)',
                        'sessionprofile_id': 'varchar(500)'},

           'products': {'_id': 'varchar(500)', "price": 'int4',
                        'name': 'varchar(500)',  'flavor': 'varchar(500)',
                        'category': 'varchar(500)', 'sub_category': 'varchar(500)', 'sub_sub_category': 'varchar(500)',
                        'sub_sub_sub_category': 'varchar(500)', 'herhaalaankopen': 'varchar(500)',
                        'recommendable': 'varchar(500)', "brand": 'varchar(500)'},

           'product_properties': {'_id': 'varchar(500)', 'doelgroep': 'varchar(500)', 'eenheid': 'varchar(500)',
                                  'gebruik': 'varchar(500)', 'serie': 'varchar(500)', 'soort': 'varchar(500)',
                                  'variant': 'varchar(500)', 'type': 'varchar(500)'},

           'sessions': {'t': 'varchar(500)', 'source': 'varchar(500)',
                        'action': 'varchar(500)', 'pagetype': 'varchar(500)', 'product': 'varchar(500)',
                        'time_on_page': 'int4', 'max_time_inactive': 'int4', 'click_count': 'int4',
                        'elements_clicked': 'int4', 'scrolls_down': 'int4', 'scrolls_up': 'int4',
                        'buid': 'varchar(500)'},

           'BUIDS': {'_id': 'varchar(500)', 'buids': 'varchar(500)'}
           }


def recursive_dict(data_design, data_fetched):
    '''
    This function takes the fetched data and assigns it to a new dictionary
    Which makes it possible to make the insert_functions a lot smoother

    :param data_design: {COLUMN: TYPE, COLUMN: TYPE, COLUMN: TYPE}
    :param data_fetched: {COLUMN: VALUE, COLUMN: VALUE, COLUMN: VALUE}
    :return: str(value with right type, value with right type)
    '''

    print(data_fetched)

    fetched_column = list(data_fetched.keys())
    fetched_value = list(data_fetched.values())

    edited_value = []
    edited_type = []

    length = len(fetched_column)

    for i in range(length):

        edited_value.append(fetched_value[i])
        edited_type.append(data_design[fetched_column[i]])

    string = ''

    teller = 0

    for i in edited_value:
        var = i
        teller += 1

        if i is None:
            string += '-1'
        elif edited_type[edited_value.index(i)] == 'varchar(500)':
            if type(i) is str or type(i) is list:
                var = str(i).replace("\'", ' ')
            string += f'\'{var}\''
        elif edited_type[edited_value.index(i)] == 'int4':
            string += f'{i}'

        if teller != (len(edited_value)):
            string += ','
    return string


def connection_mongo(host, port, database):
    '''
    Establishes  a connection with the desired MongoDB database

    All parameters need to be correct and correspond with the mongoDB for it to work
    :param host: The host in wich you have your desired database located (usually localhost)
    :param port: The port that corresponds to the host
    :param database: The database  that you'd like to access within the host
    :return: Connection with the database
    '''

    # Identifies the location of your database
    client = MongoClient(host=host, port=port)

    # Identifies the database within your host
    db = client[database]

    return db


def fetch_json_mongo(mongo_connection, dataset):
    '''
    Uses the connection_mongo function to access Json data located in your mongoDB

    :param mongo_connection: A list with mongoDB info
    :param dataset: A dictionary with the desired values and its types
    :return: A neat organized list with data

    RETURN DATA STRUCTURE
        [{'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES'},
         {'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES'},
         {'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES'}]
    '''

    # Establishes a connection with your database
    folder = mongo_connection[0]
    host = mongo_connection[1]
    port = mongo_connection[2]
    database = mongo_connection[3]
    db = connection_mongo(host, port, database)
    section = db[folder]

    # The mongoDB has many attributes, this list will make sure that
    # we only extract the attributes that we need, using an if statement
    insert_list = list(dataset[folder].keys())

    # All extracted data dictionaries will be added to this list
    return_data_final = []

    # when testing a small amount of data is inserted
    teller = 0

    # Looping through mongoDB data
    for value in section.find():

        # when testing
        teller += 1
        if teller == 1000:
            break

        # For every instance a new data dictionary is made
        return_data = {}

        # Looping through mongoDB data keys
        for value_sub in value:

            # The line's buid gets added
            try:
                buid_data = value['buid'][0]

            except:
                'niks'

            # Since the events object is a nested dictionary
            # we need to loop through it seperately
            if value_sub == 'events':

                for i in range(len(value['events'])):
                    return_data1 = dict(value['events'][i-1])
                    return_data1['buid'] = buid_data
                    return_data_final.append(return_data1)

            # Line 65, 66
            if value_sub in insert_list:

                # Since the price object is a nested dictionary
                if value_sub == 'price':
                    return_data[value_sub] = value[value_sub]['selling_price']

                # If any data is missing, a '-1' will take it's place
                elif value[value_sub] is None:
                    return_data[value_sub] = '-1'

                # Most data then gets put in as 'COLUMNS': 'VALUES'
                else:
                    return_data[value_sub] = value[value_sub]

        print(return_data)
        return_data_final.append(return_data)
    print(return_data_final)
    return return_data_final


def fetch_query_BUIDS(mongo_connection, postgres_connection):
    '''
    Makes a dictionary with BUIDS as its key and the corresponding profile_id as value

    :param mongo_connection: A list with mongoDB info
    :return: inserts the data to the BUIDS table
    '''

    # Establishes a connection with your database
    folder = 'visitors'
    host = mongo_connection[1]
    port = mongo_connection[2]
    database = mongo_connection[3]
    db = connection_mongo(host, port, database)
    section = db[folder]

    buids_dict = {}

    # All profile id's will be linked to one of their BUIDS
    # Because the BUIDS are nested in the visitors data we fetch it by calling upon the dictionary at 'buids'
    for line in section.find():
        try:
            for buid in line['buids']:
                val = str(line['_id'])
                val2 = val.replace("'", '')
                buids_dict[buid] = val
        except:
            'dan niet'

    # A connection with the Database
    conn = connection_postgres(postgres_connection[0], postgres_connection[1], postgres_connection[2],
                               postgres_connection[3], postgres_connection[4])

    cur = conn.cursor()

    # The keys act as column names, query line 1
    keys = list(buids_dict.keys())

    # It's values act as values, query line 2
    values = list(buids_dict.values())

    for i in range(len(keys)):

        # Data gets inserted
        query = (f'''INSERT INTO BUIDS (buids, _id)
VALUES (\'''''' + keys[i-1] + '''\', \'''' + values[i-1] + '''\');''')

        print(query)
        cur.execute(query)

    # When all id's are query'd, they get commited
    conn.commit()
    cur.close()
    conn.close()


def fetch_query_properties(mongo_connection, postgres_connection):
    '''

    :param mongo_connection: A list with mongoDB info
    :param buids_dict: A dictionary with
    :return:
    '''

    # Establishes a connection with your database
    folder = 'products'
    host = mongo_connection[1]
    port = mongo_connection[2]
    database = mongo_connection[3]
    db = connection_mongo(host, port, database)
    section = db[folder]

    # This list acts as a means to compare the products to each other, which we will need for our RE
    check = ['doelgroep', 'eenheid', 'gebruik', 'serie', 'soort', 'variant', 'type']

    total = []

    # We check all data in products for their properties.
    for line in section.find():

        # We start with linking it's product id to the properties
        properties_dict = {}
        properties_dict['_id'] = line['_id']

        # Now we add all properties to the right product id
        try:
            properties_fetched = line['properties']
            for i in dict(properties_fetched):
                if i in check:
                    if i is None:
                        properties_dict[i] = '-1'
                    else:
                        properties_dict[i] = line['properties'][i]

        # When there are exceptions in the data, we assign a -1 to all properties
        except:
            properties_dict['doelgroep'] = '-1'
            properties_dict['eenheid'] = '-1'
            properties_dict['gebruik'] = '-1'
            properties_dict['serie'] = '-1'
            properties_dict['soort'] = '-1'
            properties_dict['variant'] = '-1'
            properties_dict['type'] = '-1'

        total.append(properties_dict)

    # Because the value types are listed in the dataset it can easily be passed on to insert postgres
    # to prevent duplicated code
    insert_postgres('product_properties', total, postgres_connection)


def connection_postgres(host, database, user, pw, port):
    '''
    Establishes a connection with the desired PostgresQL database

    :param host: The host in wich you have your desired database located (usually localhost)
    :param database: The database that you'd like to access within the host
    :param user: Your postgres username (usually postgres)
    :param pw: Your postgres password (usually pgadmin2)
    :param port: The port that corresponds to the host
    :return:
    '''

    return psycopg2.connect(host=host, database=database, user=user, password=pw, port=port)


def table_postgres(data, connection_list):
    '''
    Takes structure from the dictionary and puts them in to a postgresQL database

    :param data:
    :param connection_list: Takes a list for the connection_postgres function
                            structured as followed, [host, database, user, pw, port]
    '''

    conn = connection_postgres(connection_list[0], connection_list[1], connection_list[2],
                               connection_list[3], connection_list[4])

    cur = conn.cursor()

    # The key of the nested dictionaries act as the table name
    for sub in data:
        query = ''''''
        query += 'CREATE TABLE ' + sub + ' ('

        # The keys and its corresponding values get queried
        teller = 0
        for sub_sub in data[sub]:
            teller += 1
            query += sub_sub + ' ' + data[sub][sub_sub]

            if teller != len(data[sub]):
                query += ', '
            else:
                query += ');'

        cur.execute(query)

    conn.commit()
    cur.close()
    conn.close()


def insert_postgres(table_name, insert_data, connection_list):
    '''
    Takes data from the dictionary and puts them in to a postgresQL database

    :param table_name: The name of the table you'd like to add data to
    :param insert_data: A dictionary consisting of columns as keys and values as values
    :param connection_list: Takes a list for the connection_postgres function
                            structured as followed, [host, database, user, pw, port]

    INSERT DATA STRUCTURE
        {'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES', 'COLUMNS': 'VALUES'}
    '''

    # A connection with the Database
    conn = connection_postgres(connection_list[0], connection_list[1], connection_list[2],
                               connection_list[3], connection_list[4])

    cur = conn.cursor()

    # For every id in the data, a query gets made
    for line in insert_data:
        dict(line)

        # The keys act as column names, query line 1
        keys = list(line.keys())

        insert_data2 = recursive_dict(dataset[table_name], line)

        separator = ", "
        skeys = separator.join(keys)

        # Data gets inserted
        query = (f'''INSERT INTO ''' + table_name + ''' (''' + skeys + ''')
VALUES (''' + insert_data2 + ''');''')

        print(query)
        cur.execute(query)
        conn.commit()

    # When all id's are query'd, they get commited
    print(' werkt')
    cur.close()
    conn.close()
