#!/usr/bin/env python3
# Austin Varghese
# 1098759
# CIS*4010
# Assignment 2 - DynamoDB, Database in the Cloud 

#
#  Libraries and Modules
#
from __future__ import print_function
import boto3
import json
import decimal
from decimal import Decimal
import csv
import pandas as pd
from boto3.dynamodb.conditions import Key, Attr

from tabulate import tabulate


#
#  Find AWS access key id and secret access key information
#  from configuration file
#

# config = configparser.ConfigParser()
# config.read("dynamodb.conf")
# aws_access_key_id = config['test']['aws_access_key_id']
# aws_secret_access_key = config['test']['aws_secret_access_key']

# USING awscli and assuming correct permissions for 'aws configure'

# dynamodb = boto3.resource('dynamodb', region_name='ca-central-1')

# get integer 
def get_int(message):

    while True:
        try:
            userInput = int(input(message))       
        except ValueError:
            print("Not an integer! Try again.")
            continue
        else:
            return userInput 
            break 

def get_float(message):

    while True:
        try:
            userInput = float(input(message))       
        except ValueError:
            print("Not a float! Try again.")
            continue
        else:
            return userInput 
            break 


# module to create a table
# only *needs* a part key, otherwise unneccessary
# partkey type can be 'N' (number), 'S' (string)


def table_create(dynamodb, tableName, partKeyName, partKeyType, sortKeyName="", sortKeyType=""):

    try:

        # if no sortkey then just add partkey
        if sortKeyName == "" or sortKeyType == "":
            KeySchema = [
                {
                    'AttributeName': partKeyName,
                    'KeyType': 'HASH' #Partition key (this wont change, partition key will always be hash)
                }
            ]

            AttributeDefinitions = [
                {
                    'AttributeName': partKeyName,
                    'AttributeType': partKeyType 
                }
            ]
        else :
            KeySchema=[
                {
                    'AttributeName': partKeyName,
                    'KeyType': 'HASH' #Partition key (this wont change, partition key will always be hash)
                },
                {
                    'AttributeName': sortKeyName,
                    'KeyType': 'RANGE' #Sort key (this wont change, sort key will always be range)
                }
            ]
            AttributeDefinitions=[
                {
                    'AttributeName': partKeyName,
                    'AttributeType': partKeyType 
                },
                {
                    'AttributeName': sortKeyName,
                    'AttributeType': sortKeyType 
                }
            ]


        table = dynamodb.create_table(
            TableName = tableName,
            KeySchema = KeySchema,
            AttributeDefinitions=AttributeDefinitions,
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )

        print("Table Status:", table.table_status, table.table_name)

        table.wait_until_exists()
        print("Successfully Created Table!")

        return 0

    except:
        print("Error Creating Table\nTable has likely already been created, check your AWS DynamoDB Tables for <" + tableName + ">")
        return 1

def table_delete(dynamodb, tableName):

    try:
        table = dynamodb.Table(tableName)
        # dynamodb.delete_table(tableName) # (this one requires client)
        table.delete()
        print("Table Status:", table.table_status, table.table_name)

        table.wait_until_not_exists()
        print("Successfully Deleted Table!")

        return 0
    
    except:
        return 1

# load records into existing table
# make seperate modules for each file?
def load_from_csv(dynamodb, dynamodb_client, econ_table, nonecon_table):

    try:
        response = dynamodb_client.describe_table(TableName=econ_table)
        response = dynamodb_client.describe_table(TableName=nonecon_table)
        load_languages(dynamodb, "./data/shortlist_languages.csv", nonecon_table)
        load_curpop(dynamodb, './data/shortlist_curpop.csv', econ_table, nonecon_table)
        load_gdppc(dynamodb, './data/shortlist_gdppc.csv', econ_table)
        load_un_shortlist(dynamodb,'./data/un_shortlist.csv',nonecon_table)
        load_capitals(dynamodb, './data/shortlist_capitals.csv', nonecon_table)
        load_area(dynamodb, './data/shortlist_area.csv', nonecon_table)

        print("\nSuccessfully loaded CSV files into", econ_table, "and",nonecon_table,"!\n")

        return 0

    except:
        print("\nError loading CSV files, make sure the tables have been created!")
        return 1

def load_languages(dynamodb, csvFile, nonecon_table):
    print("\tLoading Languages into Table...")
    with open(csvFile, 'r') as f:
        
        csvread = csv.reader(f)
        batch_data = list(csvread)
        # country_name = "Country"
        # iso3_name = ""
        # languages = ""
        for row in batch_data:
            if row == batch_data[0]:
                # this is the first row, get all attr names (plus country key name)
                # can just ignore this (continue)
                # print("first row, set the attr names as", batch_data[0][0], batch_data[0][1], batch_data[0][2])
                continue

            # print(row)
            iso3 = row[0]
            country = row[1]
            languages = row[2:]

            record = {
                'Country': country,
                'ISO3': iso3,
                'Languages': languages,
            }
            # print(record)

            # add to dynamodb table (nonecon)
            add_record(dynamodb, nonecon_table, record)


            # could update to add (only if country is there already)
            # db.update_record(dynamodb, nonecon_table, {'Country': country}, languages, "L")

def load_curpop(dynamodb, csvFile, econ_table, nonecon_table):
    print("\tLoading Currency and Population into Tables...")
    json_file = json.loads(
        pd.read_csv(csvFile).to_json(orient='records')
    )
    for record in json_file:
        population = {}
        currency = record['Currency']
        country = record['Country']

        # this is needed for gathering the year (each attr)
        for key, value in record.items():

            if key == "Currency" or key == "Country":
                continue
            else:
                # it is population
                # population.append({key : value}) # add to a list but dont want that
                if value != None:
                    population[key] = Decimal(value)
                else:
                    population[key] = value

        # could add these to the correct table after from these values
        # currency goes to economic table, while pop goes to non economic
        # print(currency)
        # print(country, population)

        cur_record = {
            'Country': country,
            'Currency': currency,
        }
        # print(record)
        # add to econ table
        # db.add_record(dynamodb, econ_table, record)

        # add to nonecon table, can use update record to add to existing
        add_record(dynamodb, econ_table, cur_record)

        # record = {
        #     'Country': country,
        #     'Population': population,
        # }
        # db.add_record(dynamodb, nonecon_table, record)

        key = {'Country': country}
        update_record(dynamodb, nonecon_table, key, "Population", population)

# just loading population table
def load_pop(dynamodb, csvFile, pop_table):
    json_file = json.loads(
        pd.read_csv(csvFile).to_json(orient='records')
    )
    for record in json_file:
        population = {}
        currency = record['Currency']
        country = record['Country']

        # this is needed for gathering the year (each attr)
        for key, value in record.items():

            if key == "Currency" or key == "Country":
                continue
            else:
                pop_record = {
                    'Country': country,
                    'Year': Decimal(key),
                    'Population': Decimal(value)
                }
                add_record(dynamodb, pop_table, pop_record)
                # it is population
                # population.append({key : value}) # add to a list but dont want that
                # if value != None:
                #     population[key] = Decimal(value)
                # else:
                #     population[key] = value

        # could add these to the correct table after from these values
        # currency goes to economic table, while pop goes to non economic
        # print(currency)
        # print(country, population)

        # cur_record = {
        #     'Country': country,
        #     'Currency': currency,
        # }
        # print(record)
        # add to econ table
        # db.add_record(dynamodb, econ_table, record)

        # add to nonecon table, can use update record to add to existing
        # add_record(dynamodb, econ_table, cur_record)

        # record = {
        #     'Country': country,
        #     'Population': population,
        # }
        # db.add_record(dynamodb, nonecon_table, record)

        # key = {'Country': country}
        # update_record(dynamodb, nonecon_table, key, "Population", population)

def load_gdppc(dynamodb, csvFile, econ_table):
    print("\tLoading GDPPC into Table...")
    json_file = json.loads(
        pd.read_csv(csvFile).to_json(orient='records')
    )
    for record in json_file:
        gdppc = {}
        country = record['Country']

        # this is needed for gathering the year (each attr)
        for key, value in record.items():

            if (key == "Country"):
                continue
            else:
                # it is gdp
                if value != None:
                    gdppc[key] = Decimal(value)
                else:
                    gdppc[key] = value

        # print(country, gdppc)

        # record = {
        #     'Country': country,
        #     'GDPPC': gdppc,
        # }
        # print(record)
        # add to econ table
        # db.add_record(dynamodb, econ_table, record)

        key = {'Country': country}
        update_record(dynamodb, econ_table, key, "GDPPC", gdppc)

def load_un_shortlist(dynamodb, csvFile, nonecon_table):
    print("\tLoading UN Shortlist into Table...")
    json_file = json.loads(
        pd.read_csv(csvFile).to_json(orient='records')
    )
    for record in json_file:
        country = record['Common Name']
        official = record['Official Name']
        iso2 = record['ISO2']

        key = {'Country': country}

        # print(country, official, iso2)

        update_record(dynamodb, nonecon_table, key, "Official Name", official)
        update_record(dynamodb, nonecon_table, key, "ISO2", iso2)

def load_capitals(dynamodb, csvFile, nonecon_table):
    print("\tLoading Capitals into Table...")
    json_file = json.loads(
        pd.read_csv(csvFile).to_json(orient='records')
    )
    for record in json_file:
        country = record['Country Name']
        capital = record['Capital']

        key = {'Country': country}

        # print(country, capital)

        update_record(dynamodb, nonecon_table, key, "Capital", capital)

def load_area(dynamodb, csvFile, nonecon_table):
    print("\tLoading Areas into Table...")
    json_file = json.loads(
        pd.read_csv(csvFile).to_json(orient='records')
    )
    for record in json_file:
        country = record['Country Name']
        area = record['Area']

        key = {'Country': country}

        # print(country, area)

        update_record(dynamodb, nonecon_table, key, "Area", area)

# Item={
#   'year': year,
#   'title': title,
#   'info': {'plot': plot, 'rating': Decimal(str(rating))}
# }
def add_record(dynamodb, tableName, record):

    try: 
        dynamodb.Table(tableName).put_item(Item = record)
        return 0
    except:
        return 1

# Key={'Country': country}
def delete_record(dynamodb, tableName, key):
    try: 
        table = dynamodb.Table(tableName)
        table.delete_item(Key=key)

        return 0
    except:
        return 1

# module to update / add a single column (can call this only after the country key is added to table)
# to update a object in an object (population at 1980): set Population.1980 = :new 
# Key={'Country': country}
def update_record(dynamodb, tableName, key, attrName, attrData):
    try: 

        table = dynamodb.Table(tableName)

        response = table.update_item(
            Key=key,
            UpdateExpression="set #a=:new",
            ExpressionAttributeNames={
                '#a': attrName
            },
            ExpressionAttributeValues={
                ':new': attrData
            },
            ReturnValues="UPDATED_NEW"
        )
        return 0
    except:
        return 1

# update the language list in dynamodb with another list
# Key={'Country': country}
def update_list(dynamodb, tableName, key, list, attrName="Languages"):
    try: 

        table = dynamodb.Table(tableName)

        response = table.update_item(
            Key=key,
            UpdateExpression="set #a=list_append(#a,:new)",
            ExpressionAttributeNames={
                '#a': attrName
            },
            ExpressionAttributeValues={
                ':new': list
            },
            ReturnValues="UPDATED_NEW"
        )
        return 0
    except:
        return 1

def update_gdp_or_pop(dynamodb, tableName, key, year, attrName, attrData):
    try: 

        table = dynamodb.Table(tableName)

        response = table.update_item(
            Key=key,
            UpdateExpression="set #a.#y = :val",
            ExpressionAttributeNames={
                '#a': attrName,
                '#y': year
            },
            ExpressionAttributeValues={
                ':val': attrData
            },
            ReturnValues="UPDATED_NEW"
        )
        return 0
    except:
        return 1

# can only be done after population and area are added to the table
# if not using year as sort key, then must access the year of the population
def set_pop_density(dynamodb, tableName, key, year, attrName="Population Density"):
    try: 

        # could query / scan for population and area in table then calculate

        population = 1000 # get number from scan/query
        area = 10000 # get number from scan/query

        density = population/area

        table = dynamodb.Table(tableName)

        response = table.update_item(
            Key=key,
            UpdateExpression="set #a.#y = :val",
            ExpressionAttributeNames={
                '#a': attrName,
                '#y': year
            },
            ExpressionAttributeValues={
                ':val': Decimal(density)
            },
            ReturnValues="UPDATED_NEW"
        )
        return 0
    except:
        return 1

# print out entire dynamodb table
def dump_table(dynamodb, tableName):
    try: 
        # scan is costly and less efficient, but it can display all data
        table = dynamodb.Table(tableName)

        response = table.scan()
        data = response['Items']

        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response['Items'])

        # create a better format for this print statement
        # for i in data:
        #     print(i)
        # print (data)
        f = open(tableName + "_dump.txt","w+")

        # dump_line = ""

        headers = []

        for i in data:
            f.write("Country: " + str(i["Country"]) + "\n")
            headers = []
            for key, value in i.items():
                headers.append(str(key))
                if key == "Country":
                    continue
                f.write("\t" + key + ": " + str(value) + "\n")

        f.close()

        # # if you would like to dump table into csv uncomment below: 

        # csvFileName = tableName + "_dump.csv"
        # f = open(csvFileName, "w+")
        # writer = csv.DictWriter(f, fieldnames=headers)
        # writer.writeheader()
        # writer.writerows(data)
        # f.close()

        print("Successfully dumped table data into " + tableName + "_dump.txt")
        print("Check your files to view")

        return 0
    except:
        print("Unable to dump table data, make sure table has been created")
        return 1

# module to get all information for a country
# should query each table
def query_country(dynamodb, tableName, country):
    try: 
        table = dynamodb.Table(tableName)
        response = table.query(
            # can just use partition key
            KeyConditionExpression=Key('Country').eq(country)
        )
        data = response['Items']
        while 'LastEvaluatedKey' in response:
            response = table.query(ExclusiveStartKey=response['LastEvaluatedKey'])
            data.update(response['Items'])

        return data
    except:
        return 1

# module sets all the ranks of the area
def set_area_rank(dynamodb, nonecon_table):
    try:
        # scan is costly and less efficient, but it can display all data
        table = dynamodb.Table(nonecon_table)

        response = table.scan()
        data = response['Items']

        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response['Items'])

        list_dict = []

        # ex_dict = {
        #     'Country': country,
        #     'Area': area,
        #     'Rank' : 1
        # }
        for i in data:
            temp_dict = {}
            area = i["Area"]
            country = i["Country"]
            # print(country, area)

            temp_dict["Country"] = country
            temp_dict["Area"] = area

            list_dict.append(temp_dict)

        sorted_list = sorted(list_dict, key=lambda i: i['Area'], reverse=True)

        rank = 1
        for item in sorted_list:
            item["Rank"] = rank

            # update each country here (add rank)
            key = {'Country' : item['Country']}
            update_record(dynamodb, nonecon_table, key, "Area Rank", rank)

            rank+=1

        # print(sorted_list)
        
        return 0
    except:
        return 1

def get_area_rank_list(dynamodb, nonecon_table):
    try:
        # scan is costly and less efficient, but it can display all data
        table = dynamodb.Table(nonecon_table)

        response = table.scan()
        data = response['Items']

        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response['Items'])

        list_dict = []

        # ex_dict = {
        #     'Country': country,
        #     'Area': area,
        #     'Rank' : 1
        # }
        for i in data:
            temp_dict = {}
            area = i["Area"]
            country = i["Country"]
            # print(country, area)

            temp_dict["Country"] = country
            temp_dict["Area"] = area

            list_dict.append(temp_dict)

        sorted_list = sorted(list_dict, key=lambda i: i['Area'], reverse=True)

        rank = 1
        for item in sorted_list:
            item["Rank"] = rank

            # update each country here (add rank)
            # key = {'Country' : item['Country']}
            # update_record(dynamodb, nonecon_table, key, "Area Rank", rank)

            rank+=1

        # print(sorted_list)
        
        return sorted_list
    except:
        return 1

def get_area_rank(dynamodb, nonecon_table, cur_country):
    try:
        area_rank_list = get_area_rank_list(dynamodb, nonecon_table)

        rank = 0

        for item in area_rank_list:
            if item["Country"] == cur_country:
                rank = item["Rank"]
        
        return rank
    except:
        return 1

# returns integer of pop rank
def get_pop_rank(dynamodb, nonecon_table, cur_country, year):
    try:

        # need to get all the populations, then compare, then return rank
        table = dynamodb.Table(nonecon_table)

        # resp = table.scan(AttributesToGet=['Country', "Population."+str(year)])

        resp = table.scan(
            ProjectionExpression= 'Country, Population.#c', 
            ExpressionAttributeNames = {'#c': str(year)}
        )['Items']

        # print(resp)

        list_dict = []

        for i in resp:
            temp_dict = {}
            country = i['Country']
            population = i['Population'][str(year)]

            if population == None: 
                population = 0

            # print(country, population)

            temp_dict["Country"] = country
            temp_dict["Population"] = population

            list_dict.append(temp_dict)

        sorted_list = sorted(list_dict, key=lambda i: i['Population'], reverse=True)

        rank = 1
        country_rank = 0
        

        for item in sorted_list:
            # item["Rank"] = rank

            # print(rank, item)

            if item["Country"] == cur_country and item["Population"] == 0:
                return 0

            elif item["Country"] == cur_country:
                country_rank = rank

            elif item["Population"] == 0:
                continue
            else:
                rank+=1
            
        # print(sorted_list)

        return country_rank
    except:
        return "error"

# returns integer of density rank
def get_density_rank(dynamodb, nonecon_table, cur_country, year):
    try:

        country_rank = 0

        # need to get all the populations, then compare, then return rank
        table = dynamodb.Table(nonecon_table)

        # resp = table.scan(AttributesToGet=['Country', "Population."+str(year)])

        resp = table.scan(
            ProjectionExpression= 'Country, Area, Population.#c', 
            ExpressionAttributeNames = {'#c': str(year)}
        )['Items']

        # print(resp)

        list_dict = []

        for i in resp:
            temp_dict = {}
            country = i['Country']
            population = i['Population'][str(year)]
            area = i['Area']

            density = 0

            if population == None or area == None: 
                density = 0
            else:
                density = population / area

            # print(density)
            # print(country, population)

            temp_dict["Country"] = country
            # temp_dict["Population"] = population
            # temp_dict["Area"] = area
            temp_dict["Density"] = density

            list_dict.append(temp_dict)

        sorted_list = sorted(list_dict, key=lambda i: i['Density'], reverse=True)
        
        # print(sorted_list)
        
        rank = 1
        country_rank = 0
        

        for item in sorted_list:
            # item["Rank"] = rank

            # print(rank, item)

            if item["Country"] == cur_country and item["Density"] == 0:
                return 0

            elif item["Country"] == cur_country:
                country_rank = rank

            elif item["Density"] == 0:
                continue
            else:
                rank+=1
            
        return country_rank
    except:
        return "error"

# returns integer of gdp rank
def get_gdp_rank(dynamodb, econ_table, cur_country, year):
    try:

        # need to get all the gdppc, then compare, then return rank
        table = dynamodb.Table(econ_table)

        # resp = table.scan(AttributesToGet=['Country', "Population."+str(year)])

        resp = table.scan(
            ProjectionExpression= 'Country, GDPPC.#c', 
            ExpressionAttributeNames = {'#c': str(year)}
        )['Items']

        # print(resp)

        list_dict = []

        for i in resp:
            temp_dict = {}
            country = i['Country']
            gdppc = i['GDPPC'][str(year)]

            if gdppc == None: 
                gdppc = 0

            # print(country, gdppc)

            temp_dict["Country"] = country
            temp_dict["GDPPC"] = gdppc

            list_dict.append(temp_dict)

        sorted_list = sorted(list_dict, key=lambda i: i['GDPPC'], reverse=True)
        
        # print(sorted_list)

        rank = 1
        country_rank = 0
        for item in sorted_list:
            # item["Rank"] = rank

            if item["Country"] == cur_country and item["GDPPC"] == 0:
                return 0

            elif item["Country"] == cur_country:
                country_rank = rank

            elif item["GDPPC"] == 0:
                continue
            else:
                rank+=1

        return country_rank
    except:
        return "error"

# module to create a select countries report
def create_country_report(dynamodb, cur_country, econ_table, nonecon_table):
    try: 
        econ_data = query_country(dynamodb, econ_table, cur_country)
        nonecon_data = query_country(dynamodb, nonecon_table, cur_country)

        # Economic Data
        country = econ_data[0]["Country"]
        currency = econ_data[0]["Currency"]
        gdppc = econ_data[0]["GDPPC"]

        # SORTING THE KEYS SO IT GOES FROM LOWEST YEAR
        myKeys = list(gdppc.keys())
        myKeys.sort()
        sorted_gdppc = {i: gdppc[i] for i in myKeys}

        # Non-Economic Data
        iso3 = nonecon_data[0]["ISO3"]
        official_name = nonecon_data[0]["Official Name"]
        capital = nonecon_data[0]["Capital"]
        languages = nonecon_data[0]["Languages"]
        area = nonecon_data[0]["Area"] # NEED TO GET THE RANK OF AREA  
        # area_rank = nonecon_data[0]["Area Rank"]
        population = nonecon_data[0]["Population"]

        area_rank = get_area_rank(dynamodb, nonecon_table, cur_country)

        # SORTING THE KEYS SO IT GOES FROM LOWEST YEAR
        myKeys = list(population.keys())
        myKeys.sort()
        sorted_pop = {i: population[i] for i in myKeys}


        # create list of dicts
        # first need YEAR - POPULATION - RANK - DENSITY - RANK

        dict_struct = {
            'Year': 2000,
            'Population': 1000,
            'Population Rank': 1,
            'Density' : 5.5,
            'Density Rank': 2
        }

        pop_list = []
        for key, attr in sorted_pop.items():
            pop_attr = attr
            pop_rank = get_pop_rank(dynamodb, nonecon_table, cur_country, key)
            density_rank = get_density_rank(dynamodb, nonecon_table, cur_country, key)

            if attr == None:
                pop_attr = 0
                continue

            pop_record = {
                'Year': key,
                'Population': pop_attr,
                'Population Rank': pop_rank,
                'Density' : float(pop_attr / area),
                'Density Rank': density_rank
            }
            pop_list.append(pop_record)
            # print(pop_record)


        dict_struct = {
            'Year': 2000,
            'GDPPC': 1000,
            'GDP Rank': 1,
        }

        gdp_list = []
        for key, attr in sorted_gdppc.items():
            gdp_attr = attr
            gdp_rank = get_gdp_rank(dynamodb, econ_table, cur_country, key)

            if attr == None:
                gdp_attr = 0
                continue

            gdp_record = {
                'Year': key,
                'GDPPC': gdp_attr,
                'Rank': gdp_rank,
            }
            gdp_list.append(gdp_record)
            # print(gdp_record)

        # WRITE TO FILE
        f = open(iso3 + "_country_report.txt","w+")

        # print(country, official_name, area, area_rank, capital, languages)
        f.write(country)
        f.write("[Official Name: "+official_name+"]\n")
        f.write("Area: "+ str(area) + " ("+ str(area_rank)+")\n")
        f.write("Official Languages: " + str(languages))
        f.write("\nCapital City: " + capital)
        
        f.write("\n\nPOPULATION\n")
        f.write("Table of Population, Population Density, and their respective world ranking for that year, ordered by year\n")
        # print(pop_list)
        # USING TABULATE, can create list of dicts and then print them
        # header = pop_list[0].keys()
        # rows =  [x.values() for x in pop_list]
        # # print(tabulate(rows, header, floatfmt=".2f", tablefmt='grid'))
        # f.write(tabulate(rows, header, floatfmt=".2f", tablefmt='grid'))

        if pop_list != []:
            header = pop_list[0].keys()
            rows =  [x.values() for x in pop_list]
            f.write(tabulate(rows, header, floatfmt=".2f", tablefmt='grid'))
        else:
            f.write("\nNO POPULATION DATA\n")

        f.write("\nECONOMICS\n")
        f.write("Currency: " + currency)
        f.write("\nTable of GDP per capita (GDPCC) and rank within the world for that year\n")
        # USING TABULATE, can create list of dicts and then print them
        # header = gdp_list[0].keys()
        # rows =  [x.values() for x in gdp_list]
        # # print(tabulate(rows, header, floatfmt=".2f", tablefmt='grid'))
        # f.write(tabulate(rows, header, floatfmt=".2f", tablefmt='grid'))

        if gdp_list != []:
            header = gdp_list[0].keys()
            rows =  [x.values() for x in gdp_list]
            f.write(tabulate(rows, header, floatfmt=".2f", tablefmt='grid'))
        else:
            f.write("\nNO GDPPC DATA\n")
        f.close()

        print("Successfully Created " + iso3 + "_country_report.txt !")
        print("Check your files to view the Country Report")
        
        return 0
    except Exception as e:
        print("Unable to create report for selected country, make sure country exists on the table")
        # print(e)
        return 1

# get sorted list of country objects and their rank
def get_pop_rank_list(dynamodb, nonecon_table,year):
    try:

        # need to get all the populations, then compare, then return rank
        table = dynamodb.Table(nonecon_table)

        # resp = table.scan(AttributesToGet=['Country', "Population."+str(year)])

        resp = table.scan(
            ProjectionExpression= 'Country, Population.#c', 
            ExpressionAttributeNames = {'#c': str(year)}
        )['Items']

        # print(resp)

        list_dict = []

        for i in resp:
            temp_dict = {}
            country = i['Country']
            population = i['Population'][str(year)]

            if population == None: 
                population = 0

            # print(country, population)

            temp_dict["Country"] = country
            temp_dict["Population"] = population

            list_dict.append(temp_dict)

        sorted_list = sorted(list_dict, key=lambda i: i['Population'], reverse=True)

        rank = 1        

        for item in sorted_list:
            # item["Rank"] = rank

            # print(rank, item)

            if item["Population"] == 0:
                item["Rank"] = 0

            else:
                item["Rank"] = rank
                rank+=1
            
        # print(sorted_list)

        return sorted_list
    except:
        return "error"

def get_density_rank_list(dynamodb, nonecon_table, year):
    try:

        # need to get all the populations, then compare, then return rank
        table = dynamodb.Table(nonecon_table)

        # resp = table.scan(AttributesToGet=['Country', "Population."+str(year)])

        resp = table.scan(
            ProjectionExpression= 'Country, Area, Population.#c', 
            ExpressionAttributeNames = {'#c': str(year)}
        )['Items']

        # print(resp)

        list_dict = []

        for i in resp:
            temp_dict = {}
            country = i['Country']
            population = i['Population'][str(year)]
            area = i['Area']

            density = 0

            if population == None or area == None: 
                density = 0
            else:
                density = population / area

            # print(density)
            # print(country, population)

            temp_dict["Country"] = country
            # temp_dict["Population"] = population
            # temp_dict["Area"] = area
            temp_dict["Density"] = density

            list_dict.append(temp_dict)

        sorted_list = sorted(list_dict, key=lambda i: i['Density'], reverse=True)
        
        # print(sorted_list)
        
        rank = 1        

        for item in sorted_list:
            # item["Rank"] = rank

            # print(rank, item)

            if item["Density"] == 0:
                item["Rank"] = 0
            else:
                item["Rank"] = rank
                rank+=1
            
        return sorted_list
    except:
        return "error"

# create global report for a given year
def create_global_report(dynamodb, econ_table, nonecon_table, cur_year):
    try:
        # need to create tables that are ordered by RANK of specific year
        # for POPULATION, AREA, DENSITY

        pop_list = get_pop_rank_list(dynamodb, nonecon_table, cur_year)
        area_list = get_area_rank_list(dynamodb, nonecon_table)
        density_list = get_density_rank_list(dynamodb, nonecon_table, cur_year)

        # need to create tables for each decade, starting at 1970
        # lists GDPPC for all countries alphabetical order
        gdp_list = gdp_ordered_list(dynamodb, econ_table)
        
        # print(gdp_list)

        # if i were to use tabulate, need a structure similar to this
        # gdp_record = {
        #     'Country Name': "country",
        #     '1970': 1,
        #     '1971': 1,
        #     '1972': 1,
        #     '1973': 1,
        #     '1974': 1,
        #     '1975': 1,
        #     '1976': 1,
        #     '1977': 1,
        #     '1978': 1,
        #     '1979': 1
        # }
        # would need to go through each country and then gather each year
        # then would need to create record with those items, and finally append to list 
        gdp_1970s = []
        gdp_1980s = []
        gdp_1990s = []
        gdp_2000s = []
        gdp_2010s = []

        country_count = 0

        for item in gdp_list:
            country_count+=1

            # get 1970s gdp
            gdp_record = {}

            gdp_record["Country"] = item["Country"]

            for year in range(1970, 1980):
                cur_gdp = item["GDPPC"][str(year)] 
                gdp_record[str(year)] = cur_gdp

            gdp_1970s.append(gdp_record)

            # get 1980s gdp
            gdp_record = {}

            gdp_record["Country"] = item["Country"]

            for year in range(1980, 1990):
                cur_gdp = item["GDPPC"][str(year)] 
                gdp_record[str(year)] = cur_gdp

            gdp_1980s.append(gdp_record)

            # get 1990s gdp
            gdp_record = {}

            gdp_record["Country"] = item["Country"]

            for year in range(1990, 2000):
                cur_gdp = item["GDPPC"][str(year)] 
                gdp_record[str(year)] = cur_gdp

            gdp_1990s.append(gdp_record)

            # get 2000s gdp
            gdp_record = {}

            gdp_record["Country"] = item["Country"]

            for year in range(2000, 2010):
                cur_gdp = item["GDPPC"][str(year)] 
                gdp_record[str(year)] = cur_gdp

            gdp_2000s.append(gdp_record)

            # get 2010s gdp
            gdp_record = {}

            gdp_record["Country"] = item["Country"]

            for year in range(2010, 2020):
                cur_gdp = item["GDPPC"][str(year)] 
                gdp_record[str(year)] = cur_gdp

            gdp_2010s.append(gdp_record)

        
        # WRITE TO FILE
        f = open(cur_year + "_global_report.txt","w+")

        f.write("Global Report\n\n")

        f.write("Year: " + str(cur_year) + "\n")
        f.write("Number of Countries: " + str(country_count) + "\n\n")
        
        f.write("Table of Countries Ranked by Population (largest to smallest)\n")
        # USING TABULATE, can create list of dicts and then print them
        header = pop_list[0].keys()
        rows =  [x.values() for x in pop_list]
        f.write(tabulate(rows, header, floatfmt=".0f", tablefmt='grid'))

        f.write("\n\nTable of Countries Ranked by Area (largest to smallest)\n")
        header = area_list[0].keys()
        rows =  [x.values() for x in area_list]
        f.write(tabulate(rows, header, floatfmt=".2f", tablefmt='grid'))

        f.write("\n\nTable of Countries Ranked by Density (largest to smallest)\n")
        header = density_list[0].keys()
        rows =  [x.values() for x in density_list]
        f.write(tabulate(rows, header, floatfmt=".2f", tablefmt='grid'))

        f.write("\n\nGDP Per Capita for all Countries\n\n")

        f.write("\n1970's Table\n")
        header = gdp_1970s[0].keys()
        rows =  [x.values() for x in gdp_1970s]
        f.write(tabulate(rows, header, floatfmt=".2f", tablefmt='grid'))


        f.write("\n1980's Table\n")
        header = gdp_1980s[0].keys()
        rows =  [x.values() for x in gdp_1980s]
        f.write(tabulate(rows, header, floatfmt=".2f", tablefmt='grid'))

        f.write("\n1990's Table\n")
        header = gdp_1990s[0].keys()
        rows =  [x.values() for x in gdp_1990s]
        f.write(tabulate(rows, header, floatfmt=".2f", tablefmt='grid'))

        f.write("\n2000's Table\n")
        header = gdp_2000s[0].keys()
        rows =  [x.values() for x in gdp_1990s]
        f.write(tabulate(rows, header, floatfmt=".2f", tablefmt='grid'))

        f.write("\n2010's Table\n")
        header = gdp_2010s[0].keys()
        rows =  [x.values() for x in gdp_1990s]
        f.write(tabulate(rows, header, floatfmt=".2f", tablefmt='grid'))

        f.close()

        print("Successfully Created " + cur_year + "_global_report.txt !")
        print("Check your files to view the Country Report")

        return 0
    except:
        print("Unable to create report for selected year, make sure years (1970 - 2019) exists on the table")
        return 1

# ORDER econ table ALPHABETICALLY
def gdp_ordered_list(dynamodb, econ_table):
    try:
        # need to get all the gdppc, then compare, then return rank
        table = dynamodb.Table(econ_table)

        # resp = table.scan(AttributesToGet=['Country', "Population."+str(year)])

        resp = table.scan(
            ProjectionExpression= 'Country, GDPPC', 
        )['Items']

        # print(resp)

        # list_dict = []

        # for i in resp:
        #     temp_dict = {}
        #     country = i['Country']
        #     gdppc = i['GDPPC']

        #     if gdppc == None: 
        #         gdppc = 0

        #     # print(country, gdppc)

        #     temp_dict["Country"] = country
        #     temp_dict["GDPPC"] = gdppc

        #     list_dict.append(temp_dict)

        sorted_list = sorted(resp, key=lambda i: i['Country'], reverse=False)

        # print(sorted_list)

        
        return sorted_list
    except:
        return 1

# get currency, as well as specific year of gdppc
def query_country_year_econ(dynamodb, econ_table, country, year):
    try: 
        data = query_country(dynamodb, econ_table, country)

        gdppc = data[0]['GDPPC'][str(year)]

        return 0
    except:
        return 1

# get all other information and specifically population of given year
def query_country_year_nonecon(dynamodb, nonecon_table, country, year):
    try: 
        data = query_country(dynamodb, nonecon_table, country)

        pop = data[0]['Population'][str(year)]

        return 0
    except:
        return 1

def get_existing_countries(dynamodb, tableName):
    try:
        table = dynamodb.Table(tableName)

        response = table.scan()
        data = response['Items']

        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response['Items'])

        country_list = []

        for i in data:
            country_list.append(i["Country"])

        return country_list
    except:
        return []
    
def get_existing_years(dynamodb, tableName, country, attrName):
    try:
        table = dynamodb.Table(tableName)

        response = table.query(KeyConditionExpression=Key('Country').eq(country))
        data = response['Items']

        year_list = []

        for key, attr in data[0][attrName].items():
            year_list.append(key)

        return year_list
    except:
        return []
    
def get_all_years(dynamodb, tableName, attrName):
    try:
        table = dynamodb.Table(tableName)

        response = table.scan()
        data = response['Items']

        year_list = []

        for key, attr in data[0][attrName].items():
            year_list.append(key)

        return year_list
    except:
        return []