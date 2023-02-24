# AWS DynamoDB Country Reports Generator

Overview:
 - create and delete DynamoDB tables
 - load DynamoDB tables from CSV files
 - create/remove specific records from table
 - add/modify existing records
 - create reports for countries and years

How to Run:
 - install necessary requirements from requirements.txt file
 - need to have a configuration file named 'dynamodb.conf' in the same directory as 'CountryReportDB.py'
 - the config file must have a user named [default] with aws_access_key_id and aws_secret_access_key
 - to run the modules, enter in terminal "python3 CountryReportDB.py"

Structure:
 - I have created a 2 python files, with one being the "main" and the other containing necessary modules
 - All the csv files are untouched other than 'shortlist_curpop.csv' and 'un_shortlist.csv'
 - - In 'shortlist_curpop.csv': I made a small change in the title for the years of 1970, previously it was "Population 1970", and I changed it to just "1970" to keep it consistent with 'shortlist_gdppc.csv'
 - - In 'un_shortlist.csv': I just added the headers as per the Slack.

How to use modules:
 - All my modules are imported from 'dbFunctions.py' into my main file of 'CountryReportDB.py'
 - I decided to make an interactive cli menu to use my modules easily, and prevent errors
 - All the modules are used within 'CountryReportDB.py' and the user has the ability to access them directly and indirectly through the program

How to generate both types of reports: 
 - It is easy to generate both reports through my cli, the user must first "[1] - Create Tables", then "[2] - Load CSV to Tables" and finally select the "[5] - Create Reports" options. 
 - From here there are two options, one to create country level report and the other to create the global report
 - These options will create a txt file that contains the necessary tables and information required
 
How to add information / modify database table:
 - Adding missing information and edits to the database tables is also very easy to do through my cli: select "[3] - Add Missing Information / Modify Existing Records"
 - Follow the steps to add/modify any existing country / year

Limitations:
 - csv files MUST have the exact same names and formatting that I currently use
 -- opening the files and parsing them requires the name of the csv files and headers to be the exact same
 - to create the reports ALL necessary data must be found for EVERY country in the country table, if population in 1980 (for example) is missing then it will not create a report
 - this essentially means that if the keys for population and gdppc from 1970 to 2019 is not available then reports will not work
 - This is because I use the year as a key within the population and gdppc column and call upon that year, so if every country doesnt contain the same years then it will break when attempting to rank a year that doesnt exist.
 - As a result I decided to preset all the keys within population with the years from 1970 to 2019 when adding a record
 - this will limit my program from allowing it to be more dynamic in the future, however it will let me easily handle creating years
 - the Global Report (Report B) requires that the years 1970 - 2019 are found within the GDPPC column in my Economic Table
 - - They can contain no values, however the year must exist ({'1980': None})
 - users can manually add records, but the population and gdppc will be preset to have all year keys from 1970-2019 set to None
 - - however, if the user has a csv with more years, the years will be set to have more if needed 
 - - the preset will make it so that a new country will have the ability to work with the global report
 - - users will not be able to add more years to population and gdppc columns


Limitations TLDR: 
 - csv files must have exact formatting of headers and naming of files given, each csv must have the same number of years and same number of countries
 - all reports require population and gdppc for each country to have the same number of years 
 - global report needs all countries to have the years 1970-2019 (minimum, they could have more but the global report needs these years present)
