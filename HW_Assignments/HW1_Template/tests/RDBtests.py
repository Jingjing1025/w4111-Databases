import src.RDBDataTable as RDBDataTable
import json


def t1():
    sql = "select * from lahman2019raw.people where nameLast=%s and birthCity=%s"
    args = ('Williams', 'San Diego')

    result = RDBDataTable.run_q(sql, args, fetch=True)

    print("Return code = ", result[0])
    print("Data = ")
    if result[1] is not None:
        print(json.dumps(result[1], indent=2))
    else:
        print("None.")


# test for RDB find_by_primary_key
def rdbtest1():
    table_name = "lahman2019raw.batting"
    connect_info = RDBDataTable._get_default_connection()
    key_columns = ["playerID", "yearID", "stint", "teamID"]
    table = RDBDataTable.RDBDataTable(table_name, connect_info, key_columns)

    key_fields = ["aardsda01", "2004", "1", "SFN"]
    field_list = ["playerId", "yearID", "stint", "teamID"]
    result = table.find_by_primary_key(key_fields, field_list)

    print("Return code = ", result[0])
    print("Data = ")
    if result[1] is not None:
        print(json.dumps(result[1], indent=2))
    else:
        print("None.")


# test for RDB find_by_template
def rdbtest2():
    table_name = "lahman2019raw.people"
    connect_info = RDBDataTable._get_default_connection()
    key_columns = ["playerID"]
    table = RDBDataTable.RDBDataTable(table_name, connect_info, key_columns)

    key_fields = ["abbotgl01"]
    field_list = ["playerId", "birthYear", "birthCity", "nameLast"]
    template = {"nameLast": "Williams", "birthCity": "San Diego"}
    result = table.find_by_template(template, field_list)

    print("Return code = ", result[0])
    print("Data = ")
    if result[1] is not None:
        print(json.dumps(result[1], indent=2))
    else:
        print("None.")


# test for RDB delete_by_key
def rdbtest3():
    table_name = "lahman2019raw.people"
    connect_info = RDBDataTable._get_default_connection()
    key_columns = ["playerID"]
    table = RDBDataTable.RDBDataTable(table_name, connect_info, key_columns)

    key_fields = ["abbotgl01"]
    result = table.delete_by_key(key_fields)

    print("Return code = ", result)


# test for RDB delete_by_template
def rdbtest4():
    table_name = "lahman2019raw.people"
    connect_info = RDBDataTable._get_default_connection()
    key_columns = ["playerID"]
    table = RDBDataTable.RDBDataTable(table_name, connect_info, key_columns)

    key_fields = ["abbotgl01"]
    field_list = ["playerId", "birthYear", "birthCity", "nameLast"]
    template = {"nameLast": "Williams", "birthCity": "San Diego"}
    result = table.delete_by_template(template)

    print("Return code = ", result)


# test for RDB update_by_key
def rdbtest5():
    table_name = "lahman2019raw.people"
    connect_info = RDBDataTable._get_default_connection()
    key_columns = ["playerID"]
    table = RDBDataTable.RDBDataTable(table_name, connect_info, key_columns)

    key_fields = ["abbotje01"]
    new_values = {"playerID": "aaaaaaa01"}
    result = table.update_by_key(key_fields, new_values)

    print("Return code = ", result)


# test for RDB update_by_template
def rdbtest6():
    table_name = "lahman2019raw.people"
    connect_info = RDBDataTable._get_default_connection()
    key_columns = ["playerID"]
    table = RDBDataTable.RDBDataTable(table_name, connect_info, key_columns)

    new_values = {"playerID": "aaaaaaa02", "birthCity": "Los Angeles"}
    template = {"nameLast": "Abbott", "birthCity": "Flint"}
    result = table.update_by_template(template, new_values)

    print("Return code = ", result)


def rdbtest7():
    table_name = "lahman2019raw.people"
    connect_info = RDBDataTable._get_default_connection()
    key_columns = ["playerID"]
    table = RDBDataTable.RDBDataTable(table_name, connect_info, key_columns)

    new_record = {"playerID": "inserttest", "birthYear": "2019", "birthCity": "New York"}
    result = table.insert(new_record)

    print("Return code = ", result)

def t2():
    table_name = "lahman2019raw.people"
    fields = ['nameLast', 'nameFirst', 'birthYear', 'birthState', 'birthMonth']
    template = {"nameLast": "Williams", "birthCity": "San Diego"}
    sql, args = RDBDataTable.create_select(table_name, template, fields)
    print("SQL = ", sql, ", args = ", args)

    result = RDBDataTable.run_q(sql, args)
    if result[1] is not None:
        print(json.dumps(result[1], indent=2))
    else:
        print("None.")


rdbtest1()
# rdbtest2()
# rdbtest3()
# rdbtest4()
# rdbtest5()
# rdbtest6()
# rdbtest7()
# t1()
# t2()
