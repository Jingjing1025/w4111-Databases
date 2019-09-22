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


def t1_load():
    print("===== Testing CSV Load Good =====")

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)

    print("Created table = " + str(csv_tbl))


def t2_find_by_primary_key():
    print("===== Testing CSV Find by Primary Key =====")

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    key_columns = ["playerID", "yearID", "stint", "teamID"]

    table = CSVDataTable("batting", connect_info, key_columns)

    key_fields = ["aardsda01", "2004", "1", "SFN"]
    field_list = ["playerID", "yearID", "stint", "teamID"]
    result = table.find_by_primary_key(key_fields, field_list)

    print("Data = ", result)


def t3_find_by_primary_key1():
    print("===== Testing CSV Find by Primary Key with no field list =====")

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    key_columns = ["playerID", "yearID", "stint", "teamID"]

    table = CSVDataTable("batting", connect_info, key_columns)

    key_fields = ["aardsda01", "2004", "1", "SFN"]
    result = table.find_by_primary_key(key_fields)

    print("Data = ", result)


def t4_find_by_template():
    print("===== Testing CSV Find by Template Good =====")

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    key_columns = ["playerID"]

    table = CSVDataTable("people", connect_info, key_columns)

    field_list = ["playerID", "birthYear", "birthState", "birthCity", "nameLast"]
    template = {"birthState": "MA", "nameLast": "Adams"}
    result = table.find_by_template(template, field_list)

    print("Data = ", result)


def t5_delete_by_primary_key():
    print("===== Testing CSV Delete by Primary Key Good =====")

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    key_columns = ["playerID"]

    table = CSVDataTable("people", connect_info, key_columns)

    key_fields = ["abbotda01"]
    result = table.delete_by_key(key_fields)

    print("Number of Rows Deleted = ", result)


def t6_delete_by_template():
    print("===== Testing CSV Delete by Template Good =====")

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    key_columns = ["playerID"]

    table = CSVDataTable("people", connect_info, key_columns)

    template = {"birthState": "MA", "nameLast": "Adams"}
    result = table.delete_by_template(template)

    print("Number of Rows Deleted = ", result)


def t7_update_by_primary_key():
    print("===== Testing CSV Update by Primary Key Good =====")

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    key_columns = ["playerID"]

    table = CSVDataTable("people", connect_info, key_columns)

    key_fields = ["abbeybe01"]
    new_values = {"playerID": "abababa01", "birthCity": "Los Angeles"}
    result = table.update_by_key(key_fields, new_values)

    print("Number of Rows Updated = ", result)


def t8_update_by_primary_key():
    print("===== Testing CSV Update by Primary Key Bad (Duplicate Key Values) =====")

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    key_columns = ["playerID"]

    table = CSVDataTable("people", connect_info, key_columns)

    key_fields = ["abbated01"]
    new_values = {"playerID": "abababa01", "birthCity": "Los Angeles"}
    result = table.update_by_key(key_fields, new_values)

    print("Number of Rows Updated = ", result)


def t9_update_by_template():
    print("===== Testing CSV Update by Template Good =====")

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    key_columns = ["playerID"]

    table = CSVDataTable("people", connect_info, key_columns)

    new_values = {"birthYear": "1997", "birthCity": "Los Angeles"}
    template = {"nameLast": "Abbott"}
    result = table.update_by_template(template, new_values)

    print("Number of Rows Updated = ", result)


def t10_update_by_template():
    print("===== Testing CSV Update by Template Bad (Duplicate Key Values) =====")

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    key_columns = ["playerID"]

    table = CSVDataTable("people", connect_info, key_columns)

    new_values = {"playerID": "aaronha01", "birthYear": "1997", "birthCity": "Los Angeles"}
    template = {"nameLast": "Abbott"}
    result = table.update_by_template(template, new_values)

    print("Number of Rows Updated = ", result)


def t11_insert():
    print("===== Testing CSV Insert Good =====")

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    key_columns = ["playerID"]

    table = CSVDataTable("people", connect_info, key_columns)
    new_record = {"playerID": "inserttest01", "birthYear": "2019", "birthCity": "New York"}
    table.insert(new_record)


def t12_insert():
    print("===== Testing CSV Insert Bad (Invalid Columns) =====")

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    key_columns = ["playerID"]

    table = CSVDataTable("people", connect_info, key_columns)
    new_record = {"player": "inserttest01", "birthYear": "2019", "birthCity": "New York"}
    table.insert(new_record)


def t13_find_by_primary_key2():
    print("===== Testing CSV Find by Primary Key after insertion =====")

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    key_columns = ["playerID"]

    table = CSVDataTable("people", connect_info, key_columns)

    key_fields = ["inserttest01"]
    result = table.find_by_primary_key_list(key_fields)

    print("Data = ", result)


# t1_load()
# t2_find_by_primary_key()
# t3_find_by_primary_key1()
# t4_find_by_template()
# t5_delete_by_primary_key()
# t6_delete_by_template()
# t7_update_by_primary_key()
# t8_update_by_primary_key()
t9_update_by_template()
# t10_update_by_template()
# t11_insert()
# t12_insert()
# t13_find_by_primary_key2()
