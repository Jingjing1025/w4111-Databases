from src.CSVDataTable import CSVDataTable
import logging
import os


# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.DEBUG)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("../Data/Baseball")


def t_load():

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)

    print("Created table = " + str(csv_tbl))


# t_load()


def t1_find_by_primary_key():
    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    key_columns = ["playerID", "yearID", "stint", "teamID"]

    table = CSVDataTable("batting", connect_info, key_columns)

    key_fields = ["aardsda01", "2004", "1", "SFN"]
    field_list = ["playerID", "yearID", "stint", "teamID"]
    result = table.find_by_primary_key(key_fields, field_list)

    print(result)

# t1_find_by_primary_key()


def t1_find_by_primary_key1():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    key_columns = ["playerID"]

    table = CSVDataTable("people", connect_info, key_columns)

    key_fields = ["inserttest"]
    field_list = ["playerID", "yearID", "stint", "teamID"]
    result = table.find_by_primary_key(key_fields)

    print(result)

# t1_find_by_primary_key1()


def t2_find_by_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    key_columns = ["playerID"]

    table = CSVDataTable("people", connect_info, key_columns)

    field_list = ["playerID", "birthYear", "birthCity", "nameLast"]
    # template = {"nameLast": "Williams", "birthCity": "San Diego"}
    template = {"nameLast": "Abbott"}
    result = table.find_by_template(template, field_list)

    print(result)
    print(len(result))

# t2_find_by_template()

def t3_delete_by_primary_key():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    key_columns = ["playerID"]

    table = CSVDataTable("people", connect_info, key_columns)

    key_fields = ["abbotgl01"]
    result = table.delete_by_key(key_fields)

    print(result)

# t3_delete_by_primary_key()

def t4_delete_by_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    key_columns = ["playerID"]

    table = CSVDataTable("people", connect_info, key_columns)

    template = {"nameLast": "Williams", "birthCity": "San Diego"}
    result = table.delete_by_template(template)

    print(result)

# t4_delete_by_template()

def t5_update_by_primary_key():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    key_columns = ["playerID"]

    table = CSVDataTable("people", connect_info, key_columns)

    key_fields = ["abbotje01"]
    new_values = {"playerID": "aaaaaaa01", "birthCity": "Los Angeles"}
    result = table.update_by_key(key_fields, new_values)

    print(result)

# t5_update_by_primary_key()

def t6_update_by_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    key_columns = ["playerID"]

    table = CSVDataTable("people", connect_info, key_columns)

    new_values = {"birthYear": "2019", "birthCity": "Los Angeles"}
    template = {"nameLast": "Abbott"}
    result = table.update_by_template(template, new_values)

    print(result)

# t6_update_by_template()

def t7_insert():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    key_columns = ["playerID"]

    table = CSVDataTable("people", connect_info, key_columns)
    new_record = {"playerID": "inserttest", "birthYear": "2019", "birthCity": "New York"}
    result = table.insert(new_record)

    print(result)

# t7_insert()