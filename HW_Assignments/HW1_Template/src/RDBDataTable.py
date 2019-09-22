from src.BaseDataTable import BaseDataTable
import pymysql


def _get_default_connection():
    result = pymysql.connect(host='localhost',
                             user='dbuser',
                             password='dbuserdbuser',
                             db='lahman2019raw',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
    return result


def run_q(sql, args=None, fetch=True, cur=None, conn=None, commit=True):
    """
    Helper function to run an SQL statement.

    :param sql: SQL template with placeholders for parameters.
    :param args: Values to pass with statement.
    :param fetch: Execute a fetch and return data.
    :param conn: The database connection to use. The function will use the default if None.
    :param cur: The cursor to use. This is wizard stuff. Do not worry about it for now.
    :param commit: This is wizard stuff. Do not worry about it.

    :return: A tuple of the form (execute response, fetched data)
    """

    cursor_created = False
    connection_created = False

    try:
        if conn is None:
            connection_created = True
            conn = _get_default_connection()

        if cur is None:
            cursor_created = True
            cur = conn.cursor()

        # if args is not None:
        #     log_message = cur.mogrify(sql, args)
        # else:
        #     log_message = sql
        #
        # logger.debug("Executing SQL = " + log_message)

        res = cur.execute(sql, args)

        if fetch:
            data = cur.fetchall()
        else:
            data = None

        if commit is True:
            conn.commit()

    except Exception as e:
        raise e

    return res, data


def keycolumns_to_where_clause(keys):
    """

    :param keys: list of key_columns that needs to be matched
    :return: WHERE clause corresponding to the key_columns.
    """

    w_clause = ""

    if keys is None or keys == {}:
        result = None
    else:
        terms = []

        for k in keys:
            terms.append(" " + k + "=%s ")

        w_clause = "AND".join(terms)
        w_clause = " WHERE " + w_clause

    return w_clause


def template_to_where_clause(template):
    """

    :param template: One of those weird templates
    :return: WHERE clause corresponding to the template.
    """

    if template is None or template == {}:
        result = (None, None)
    else:
        args = []
        terms = []

        for k, v in template.items():
            terms.append(" " + k + "=%s ")
            args.append(v)

        w_clause = "AND".join(terms)
        w_clause = " WHERE " + w_clause

        result = (w_clause, args)

    return result


def create_select(table_name, template, fields, order_by=None, limit=None, offset=None):
    """
    Produce a select statement: sql string and args.

    :param table_name: Table name: May be fully qualified dbname.tablename or just tablename.
    :param fields: Columns to select (an array of column name)
    :param template: One of Don Ferguson's weird JSON/python dictionary templates.
    :param order_by: Ignore for now.
    :param limit: Ignore for now.
    :param offset: Ignore for now.
    :return: A tuple of the form (sql string, args), where the sql string is a template.
    """

    if fields is None:
        field_list = " * "
    else:
        field_list = " " + ",".join(fields) + " "

    w_clause, args = template_to_where_clause(template)

    sql = "select " + field_list + " from " + table_name + " " + w_clause

    return sql, args


def create_insert(table_name, row):
    """

    :param table_name: A table name, which may be fully qualified.
    :param row: A Python dictionary of the form: { ..., "column_name" : value, ...}
    :return: SQL template string, args for insertion into the template
    """

    result = "Insert into " + table_name + " "
    cols = []
    vals = []

    # This is paranoia. I know that calling keys() and values() should return in matching order,
    # but in the long term only the paranoid survive.
    for k, v in row.items():
        cols.append(k)
        vals.append(v)

    col_clause = "(" + ",".join(cols) + ") "

    no_cols = len(cols)
    terms = ["%s"] * no_cols
    terms = ",".join(terms)
    value_clause = " values (" + terms + ")"

    result += col_clause + value_clause

    return result, vals


def create_update(table_name, new_values, template):
    """

    :param table_name: A table name, which may be fully qualified.
    :param new_values: A dictionary containing cols and the new values.
    :param template: A template to form the where clause.
    :return: An update statement template and args.
    """
    set_terms = []
    args = []

    for k, v in new_values.items():
        set_terms.append(k + "=%s")
        args.append(v)

    s_clause = ",".join(set_terms)
    w_clause, w_args = template_to_where_clause(template)

    # There are %s in the SET clause and the WHERE clause. We need to form
    # the combined args list.
    args.extend(w_args)

    sql = "update " + table_name + " set " + s_clause + " " + w_clause

    return sql, args


class RDBDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    def __init__(self, table_name, connect_info, key_columns):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._table_name = table_name
        self._connect_info = connect_info
        self._key_columns = key_columns

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """

        if key_fields is None or len(key_fields) is 0:
            return None

        if field_list is None:
            fields = " * "
        else:
            fields = " " + ",".join(field_list) + " "

        w_clause = keycolumns_to_where_clause(self._key_columns)
        sql = "select " + fields + " from " + self._table_name + " " + w_clause
        args = key_fields

        res, data = run_q(sql, args)
        return data

    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """

        if template is None or len(template) is 0:
            return None

        if field_list is None:
            fields = " * "
        else:
            fields = " " + ",".join(field_list) + " "

        w_clause, args = template_to_where_clause(template)
        sql = "select " + fields + " from " + self._table_name + " " + w_clause

        res, data = run_q(sql, args, conn=self._connect_info)
        return data

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param key_fields: list of key_fields that need to be matched.
        :return: A count of the rows deleted.
        """

        if key_fields is None or len(key_fields) is 0:
            return 0

        w_clause = keycolumns_to_where_clause(self._key_columns)
        sql = "delete from " + self._table_name + " " + w_clause
        args = key_fields

        res = run_q(sql, args, conn=self._connect_info)
        return res[0]

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """

        if template is None or len(template) is 0:
            return 0

        w_clause, args = template_to_where_clause(template)
        sql = "delete from " + self._table_name + " " + w_clause

        res = run_q(sql, args, fetch=False, commit=True, conn=self._connect_info)
        return res[0]

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """

        if key_fields is None or len(key_fields) is 0 or new_values is None or len(new_values) is 0:
            return 0

        set_terms = []
        args = []

        for k, v in new_values.items():
            set_terms.append(k + "=%s")
            args.append(v)

        s_clause = ",".join(set_terms)
        w_clause = keycolumns_to_where_clause(self._key_columns)

        # Form combined args for SET and WHERE
        args.extend(key_fields)

        sql = "update " + self._table_name + " set " + s_clause + " " + w_clause

        res = run_q(sql, args, fetch=False, commit=True, conn=self._connect_info)

        return res[0]

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """

        if template is None or len(template) is 0 or new_values is None or len(new_values) is 0:
            return 0

        sql, args = create_update(self._table_name, new_values, template)

        res = run_q(sql, args, fetch=False, commit=True, conn=self._connect_info)

        return res[0]

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """

        if new_record is None or len(new_record) is 0:
            return

        result, vals = create_insert(self._table_name, new_record)

        res = run_q(result, vals, conn=self._connect_info)

        return

    def get_rows(self):
        return self._rows
