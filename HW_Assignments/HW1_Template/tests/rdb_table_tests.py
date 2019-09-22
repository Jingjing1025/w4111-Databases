import src.RDBDataTable as RDBDataTable
import json


def t1_load():
    print("===== Testing RDB Load Good (limit 5) =====")

    sql = "select * from lahman2019raw.people limit 5"

    result = RDBDataTable.run_q(sql, fetch=True)

    print("Return code = ", result[0])
    print("Data = ")
    if result[1] is not None:
        print(json.dumps(result[1], indent=2))
    else:
        print("None.")


def t2_find_by_primary_key():
    print("===== Testing RDB Find by Primary Key =====")

    table_name = "lahman2019raw.batting"
    connect_info = RDBDataTable._get_default_connection()
    key_columns = ["playerID", "yearID", "stint", "teamID"]
    table = RDBDataTable.RDBDataTable(table_name, connect_info, key_columns)

    key_fields = ["aardsda01", "2004", "1", "SFN"]
    field_list = ["playerId", "yearID", "stint", "teamID"]
    result = table.find_by_primary_key(key_fields, field_list)

    print("Data = ")
    if result is not None:
        print(json.dumps(result, indent=2))
    else:
        print("None.")


def t3_find_by_primary_key():
    print("===== Testing RDB Find by Primary Key with no field list =====")

    table_name = "lahman2019raw.batting"
    connect_info = RDBDataTable._get_default_connection()
    key_columns = ["playerID", "yearID", "stint", "teamID"]
    table = RDBDataTable.RDBDataTable(table_name, connect_info, key_columns)

    key_fields = ["aardsda01", "2004", "1", "SFN"]
    result = table.find_by_primary_key(key_fields)

    print("Data = ")
    if result is not None:
        print(json.dumps(result, indent=2))
    else:
        print("None.")


def t4_find_by_template():
    print("===== Testing RDB Find by Template Good =====")

    table_name = "lahman2019raw.people"
    connect_info = RDBDataTable._get_default_connection()
    key_columns = ["playerID"]
    table = RDBDataTable.RDBDataTable(table_name, connect_info, key_columns)

    field_list = ["playerID", "birthYear", "birthState", "birthCity", "nameLast"]
    template = {"birthState": "MA", "nameLast": "Adams"}
    result = table.find_by_template(template, field_list)

    print("Data = ")
    if result is not None:
        print(json.dumps(result, indent=2))
    else:
        print("None.")


def t5_delete_by_key():
    print("===== Testing RDB Delete by Primary Key Good =====")

    table_name = "lahman2019raw.people"
    connect_info = RDBDataTable._get_default_connection()
    key_columns = ["playerID"]
    table = RDBDataTable.RDBDataTable(table_name, connect_info, key_columns)

    key_fields = ["abbotda01"]
    result = table.delete_by_key(key_fields)

    print("Number of Rows Deleted = ", result)


def t6_delete_by_template():
    print("===== Testing RDB Delete by Template Good =====")

    table_name = "lahman2019raw.people"
    connect_info = RDBDataTable._get_default_connection()
    key_columns = ["playerID"]
    table = RDBDataTable.RDBDataTable(table_name, connect_info, key_columns)

    template = {"birthState": "MA", "nameLast": "Adams"}
    result = table.delete_by_template(template)

    print("Number of Rows Deleted = ", result)


def t7_update_by_key():
    print("===== Testing RDB Update by Primary Key Good =====")

    table_name = "lahman2019raw.people"
    connect_info = RDBDataTable._get_default_connection()
    key_columns = ["playerID"]
    table = RDBDataTable.RDBDataTable(table_name, connect_info, key_columns)

    key_fields = ["abbeybe01"]
    new_values = {"playerID": "abababa01", "birthCity": "Los Angeles"}
    result = table.update_by_key(key_fields, new_values)

    print("Number of Rows Updated = ", result)


def t8_update_by_key():
    print("===== Testing RDB Update by Primary Key Bad (Duplicate Key Values) =====")

    table_name = "lahman2019raw.people"
    connect_info = RDBDataTable._get_default_connection()
    key_columns = ["playerID"]
    table = RDBDataTable.RDBDataTable(table_name, connect_info, key_columns)

    key_fields = ["abbated01"]
    new_values = {"playerID": "abababa01", "birthCity": "Los Angeles"}
    result = table.update_by_key(key_fields, new_values)

    print("Number of Rows Updated = ", result)


# test for RDB update_by_template
def t9_update_by_template():
    print("===== Testing RDB Update by Template Good =====")

    table_name = "lahman2019raw.people"
    connect_info = RDBDataTable._get_default_connection()
    key_columns = ["playerID"]
    table = RDBDataTable.RDBDataTable(table_name, connect_info, key_columns)

    new_values = {"birthYear": "1997", "birthCity": "Los Angeles"}
    template = {"nameLast": "Abbott"}
    result = table.update_by_template(template, new_values)

    print("Number of Rows Updated = ", result)


def t10_update_by_template():
    print("===== Testing RDB Update by Template Bad (Duplicate Key Values) =====")

    table_name = "lahman2019raw.people"
    connect_info = RDBDataTable._get_default_connection()
    key_columns = ["playerID"]
    table = RDBDataTable.RDBDataTable(table_name, connect_info, key_columns)

    new_values = {"playerID": "aaronha01", "birthYear": "1997", "birthCity": "Los Angeles"}
    template = {"nameLast": "Abbott"}
    result = table.update_by_template(template, new_values)

    print("Number of Rows Updated = ", result)


def t11_insert():
    print("===== Testing RDB Insert Good =====")

    table_name = "lahman2019raw.people"
    connect_info = RDBDataTable._get_default_connection()
    key_columns = ["playerID"]
    table = RDBDataTable.RDBDataTable(table_name, connect_info, key_columns)

    new_record = {"playerID": "inserttest01", "birthYear": "2019", "birthCity": "New York"}
    table.insert(new_record)


def t12_insert():
    print("===== Testing RDB Insert Bad (Invalid Columns) =====")

    table_name = "lahman2019raw.people"
    connect_info = RDBDataTable._get_default_connection()
    key_columns = ["playerID"]
    table = RDBDataTable.RDBDataTable(table_name, connect_info, key_columns)

    new_record = {"player": "inserttest01", "birthYear": "2019", "birthCity": "New York"}
    table.insert(new_record)


def t13_find_by_primary_key():
    print("===== Testing RDB Find by Primary Key after insertion =====")

    table_name = "lahman2019raw.people"
    connect_info = RDBDataTable._get_default_connection()
    key_columns = ["playerID"]
    table = RDBDataTable.RDBDataTable(table_name, connect_info, key_columns)

    key_fields = ["inserttest01"]
    result = table.find_by_primary_key(key_fields)

    print("Data = ")
    if result is not None:
        print(json.dumps(result, indent=2))
    else:
        print("None.")


# t1_load()
# t2_find_by_primary_key()
# t3_find_by_primary_key()
# t4_find_by_template()
# t5_delete_by_key()
# t6_delete_by_template()
# t7_update_by_key()
# t8_update_by_key()
# t9_update_by_template()
# t10_update_by_template()
# t11_insert()
# t12_insert()
t13_find_by_primary_key()
