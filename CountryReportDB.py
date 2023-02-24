#!/usr/bin/env python3
# Austin Varghese
# 1098759
# CIS*4010
# Assignment 2 - DynamoDB, Database in the Cloud 

#
#  Libraries and Modules
#
from __future__ import print_function
import configparser
import boto3
import pandas as pd
import json
import decimal
from decimal import Decimal
import csv
from boto3.dynamodb.conditions import Key, Attr

import dbFunctions as db

#
#  Find AWS access key id and secret access key information
#  from configuration file
#
config = configparser.ConfigParser()
config.read("dynamodb.conf")
aws_access_key_id = config['default']['aws_access_key_id']
aws_secret_access_key = config['default']['aws_secret_access_key']

# if aws configure completed just need this
dynamodb_client = boto3.client('dynamodb', region_name='ca-central-1')
dynamodb = boto3.resource('dynamodb', region_name='ca-central-1')

# EXAMPLE OBJECTS TO BE PUT INTO TABLE

country_nonecon = {
    'Country': "China",
    'Languages': ["Chinese", "Mandarin"],
    'Population': {
        '1970': 123,
        '2000': 12312
    },
    'Area': 2000,
    'Official Name': "official",
    'ISO3': "iso 3",
    'ISO2': "iso 2",
    'Capital': "cap",
    'Area Rank': 1,
    # not sure if density will stay in here or just on the table
    'Population Density': {
        '1970': 123,
        '2000': 12312
    },
    'Density Rank': 1
}

country_econ = {
    'Country': "China",
    'GDPPC': {
        '1970': 123,
        '2000': 12312
    },
    'Currency': "cur",
    'GDPPC Rank': 1
}


# TABLE NAMES

nonecon_table = "avargh01_countries_nonecon"
econ_table = "avargh01_countries_econ"

try:
#
#  Establish an AWS session
#
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
#
#  Set up dynamodb resource
#
    dynamodb_client = session.client('dynamodb', region_name='ca-central-1') 
    dynamodb = session.resource('dynamodb', region_name='ca-central-1')

    print ( "Welcome to the AWS DynamoDB" )
    print ( "You are now connected to your DynamoDB Tables" )

    # Menu to access all required modules start here

    arg = ""
    while (arg != "Q" and arg != "q"):   
        print("\nDynamoDB Country Report")
        print("Select an option below: ")
        print("[1] - Create Tables")
        print("[2] - Delete Tables")
        print("[3] - Load CSV to Tables")
        print("[4] - Add Record")
        print("[5] - Delete Record") # delete a country from table
        print("[6] - Dump Table Data") # user chooses econ / nonecon
        print("[7] - Create Reports")
        print("[8] - Add Missing Information / Modify Existing Records")

        print("\n[Q] - Quit Program\n")
        arg = input("> ")

        # CREATING TABLES [COMPLETED]
        if arg == "1":
            print("Creating Economic and Non-Economic Country Tables")
            ret = db.table_create(dynamodb, nonecon_table, "Country", "S")
            ret = db.table_create(dynamodb, econ_table, "Country", "S")
            # ret = db.table_create(dynamodb, pop_table, "Country", "S", "Year", "N")

        # LOADING CSV [COMPLETED]
        elif arg == "3":

            print("Loading data into Economic and Non-Economic Country Tables")
            # PARSE EACH FILE / ADD TO TABLES

            ret = db.load_from_csv(dynamodb, dynamodb_client, econ_table, nonecon_table)

            # # not going to use this, not worth to keep this data on table
            # # db.set_area_rank(dynamodb, nonecon_table)

        # ADDING MISSING INFO / MODIFY
        elif arg == "8":
            print("What Country would you like to add to? (Case Sensitive)")
            country = input("> ")

            country_list = db.get_existing_countries(dynamodb, econ_table)

            if country not in country_list:
                print("Country not found in DynamoDB Tables")
            else:
                    

                key = {'Country' : country}

                print("\nSelect Information to Add / Modify")
                
                print("[1] - Languages")
                print("[2] - Area")
                print("[3] - Capital")
                print("[4] - Population")
                print("[5] - Official Name")
                print("[6] - ISO2")
                print("[7] - ISO3")
                print("[8] - Currency")
                print("[9] - GDPPC")
                opt = input("> ")

                if opt == "1":
                    languages = []
                    lang_opt = "1"
                    while (lang_opt == "1"):
                        lang = input("Enter Language to add to "+country+": ")
                        languages.append(lang)
                        print("\nWould you like to add more languages to "+country+"?")
                        print("[1] - Add more")
                        print("[2] - Stop")
                        lang_opt = input("> ")

                    ret = db.update_list(dynamodb, nonecon_table, key, languages )
                    if ret == 0:
                        print("Successfully added languages to " + country)
                    else:
                        print("Unable to add languages to " + country)

                if opt == "2":
                    area = db.get_float("Enter Area: ")
                    ret = db.update_record(dynamodb, nonecon_table, key, "Area", Decimal(str(area)) )
                    if ret == 0:
                        print("Successfully changed Area for " + country)
                    else:
                        print("Unable to change area for " + country)

                if opt == "3":
                    capital = input("Enter Capital: ")
                    ret = db.update_record(dynamodb, nonecon_table, key, "Capital", capital )
                    if ret == 0:
                        print("Successfully changed capital for " + country)
                    else:
                        print("Unable to change capital for " + country)

                if opt == "4":
                    year = db.get_int("Enter year of population to change: ")
                    year_list = db.get_existing_years(dynamodb, nonecon_table, country, "Population")

                    if str(year) not in year_list:
                        print("Invalid year, year must be present on DynamoDB Table")
                    else:
                        population = db.get_int("Enter the population: ")

                        ret = db.update_gdp_or_pop(dynamodb, nonecon_table, key, str(year), "Population", population )
                        if ret == 0:
                            print("Successfully changed population for " + country + " " + str(year))
                        else:
                            print("Unable to change population for " + country)
                        
                if opt == "5":
                    off_name = input("Enter Official Name: ")

                    ret = db.update_record(dynamodb, nonecon_table, key, "Official Name", off_name )
                    if ret == 0:
                        print("Successfully changed population for " + country)
                    else:
                        print("Unable to change population for " + country)

                if opt == "6":
                    iso2 = input("Enter ISO2: ")

                    ret = db.update_record(dynamodb, nonecon_table, key, "ISO2", iso2 )
                    if ret == 0:
                        print("Successfully changed ISO2 for " + country)
                    else:
                        print("Unable to change ISO2 for " + country)

                if opt == "7":
                    iso3 = input("Enter ISO3: ")

                    ret = db.update_record(dynamodb, nonecon_table, key, "ISO3", iso3 )
                    if ret == 0:
                        print("Successfully changed ISO3 for " + country)
                    else:
                        print("Unable to change ISO3 for " + country)

                if opt == "8":
                    currency = input("Enter Currency: ")

                    ret = db.update_record(dynamodb, econ_table, key, "Currency", currency )
                    if ret == 0:
                        print("Successfully changed currency for " + country)
                    else:
                        print("Unable to change currency for " + country)

                if opt == "9":
                    year = db.get_int("Enter year of GDPPC to change: ")
                    year_list = db.get_existing_years(dynamodb, econ_table, country, "GDPPC")

                    if str(year) not in year_list:
                        print("Invalid year, year must be present on DynamoDB Table")
                    else:

                        gdppc = db.get_float("Enter the GDPPC: ")

                        ret = db.update_gdp_or_pop(dynamodb, econ_table, key, str(year), "GDPPC", Decimal(str(gdppc)) )
                        if ret == 0:
                            print("Successfully changed GDPPC for " + country + " " + str(year))
                        else:
                            print("Unable to change GDPPC for " + country)

        # DUMP TABLE [COMPLETED]
        elif arg == "6":
            print("What Table would you like to dump")
            print("[1] - Economic Country Table")
            print("[2] - Non-Economic Country Table")
            arg5 = input("> ")

            if arg5 == "1":
                ret = db.dump_table(dynamodb, econ_table)
            elif arg5 == "2":
                ret = db.dump_table(dynamodb, nonecon_table)

        # CREATE REPORTS [COMPLETED]
        elif arg == "7":
            print("What Report would you like to create")
            print("[1] - Country Level Report")
            print("[2] - Global Report")
            arg6 = input("> ")

            if arg6 == "1":
                country = input("Enter Country (case sensitive): ")
                ret = db.create_country_report(dynamodb, country, econ_table, nonecon_table)

            if arg6 == "2":
                year = input("Enter Year: ")
                ret = db.create_global_report(dynamodb, econ_table, nonecon_table, year)

        # DELETING TABLES [COMPLETED]
        elif arg == "2":
            print("Deleting Economic and Non-Economic Tables")
            ret = db.table_delete(dynamodb, nonecon_table)
            ret = db.table_delete(dynamodb, econ_table)
            # ret = db.table_delete(dynamodb, pop_table)

        # DELETING RECORD [COMPLETED]
        elif arg == "5":
            print("Enter a Country you would like to delete from DynamoDB Tables (Case Sensitive): ") 
            arg8 = input("> ")

            print("Removing ["+arg8+"] from Economic and Non-Economic Tables")
            ret = db.delete_record(dynamodb, econ_table, {'Country': arg8})
            ret = db.delete_record(dynamodb, nonecon_table, {'Country': arg8})

        # ADDING RECORD 
        elif arg == "4":
            print("Enter a Country you would like to add to DynamoDB Tables (Case Sensitive): ") 
            country = input("> ")

            country_list = db.get_existing_countries(dynamodb, econ_table)

            # if len(country_list) == 0:
            #     print("Must load countries through csv first")

            if country not in country_list:

                populations = {}
                gdppcs = {}

                default_year_list = []
                for y in range(1970,2020):
                    default_year_list.append(y)

                # for y in range(1970,2020):
                #     populations[str(y)] = None
                #     gdppcs[str(y)] = None

                year_list = db.get_all_years(dynamodb, econ_table, "GDPPC")
                # for y in year_list:
                #     populations[y] = None
                #     gdppcs[y] = None

                if len(year_list) > len(default_year_list):
                    for y in year_list:
                        populations[y] = None
                        gdppcs[y] = None
                else:
                    for y in range(1970,2020):
                        populations[str(y)] = None
                        gdppcs[str(y)] = None

                

                print("Fill out all information below to create new country record!")
                print("\t* Population and GDP will be set to None for each year *")
                print("\t* To add the Population/GDP, go to 'Add Missing Information / Modify Existing Records' *\n")

                print("Non-Economic Data:")
                area = db.get_float("\tEnter Area: ")
                capital = input("\tEnter Capital: ")
                iso2 = input("\tEnter ISO2: ")
                iso3 = input("\tEnter ISO3: ")
                off_name = input("\tEnter Official Name: ")

                languages = []
                lang_opt = "1"
                while (lang_opt == "1"):
                    lang = input("\tEnter Language to add to "+country+": ")
                    languages.append(lang)
                    print("\n\tWould you like to add more languages to "+country+"?")
                    print("\t[1] - Add more")
                    print("\t[2] - Stop")
                    lang_opt = input("\t> ")

                print("Economic Data:")
                currency = input("\tEnter Currency: ")

                nonecon_rec = {
                    'Country': country,
                    'Area': Decimal(str(area)),
                    'Capital':capital,
                    'ISO2': iso2,
                    'ISO3': iso3,
                    'Languages': languages,
                    'Official Name': off_name,
                    'Population': populations
                }

                econ_rec = {
                    'Country': country,
                    'Currency': currency,
                    'GDPPC': gdppcs
                }

                print("Adding " + country + " to DynamoDB tables...!")

                ret = db.add_record(dynamodb, nonecon_table, nonecon_rec)
                if ret == 0:
                    print("Successfully added record to <" + nonecon_table+ "> table!")
                else:
                    print("Failed to add record <" + nonecon_table+ "> table")

                ret = db.add_record(dynamodb, econ_table, econ_rec)
                if ret == 0:
                    print("Successfully added record to <" + econ_table+ "> table!")
                else:
                    print("Failed to add record <" + econ_table+ "> table")

                
            else:
                print("Country already found in DynamoDB Tables")
                
                

            # print("Removing ["+arg8+"] from Economic and Non-Economic Tables")
            # ret = db.delete_record(dynamodb, econ_table, {'Country': arg8})
            # ret = db.delete_record(dynamodb, nonecon_table, {'Country': arg8})


except Exception as e:
    print ( "You could not be connected to your DynamoDB storage" )
    print ( "Please review procedures for authenticating your account" )
    print(e)