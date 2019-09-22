from src.BaseDataTable import BaseDataTable
import copy
import csv
import logging
import json
import os
import pandas as pd

pd.set_option("display.width", 256)
pd.set_option('display.max_columns', 20)


class CSVDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    _rows_to_print = 10
    _no_of_separators = 2

    def __init__(self, table_name, connect_info, key_columns, debug=True, load=True, rows=None):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
            "debug": debug
        }

        self._logger = logging.getLogger()

        self._logger.debug("CSVDataTable.__init__: data = " + json.dumps(self._data, indent=2))

        if rows is not None:
            self._rows = copy.copy(rows)
        else:
            self._rows = []
            self._load()

    def __str__(self):

        result = "CSVDataTable: config data = \n" + json.dumps(self._data, indent=2)

        no_rows = len(self._rows)
        if no_rows <= CSVDataTable._rows_to_print:
            rows_to_print = self._rows[0:no_rows]
        else:
            temp_r = int(CSVDataTable._rows_to_print / 2)
            rows_to_print = self._rows[0:temp_r]
            keys = self._rows[0].keys()

            for i in range(0, CSVDataTable._no_of_separators):
                tmp_row = {}
                for k in keys:
                    tmp_row[k] = "***"
                rows_to_print.append(tmp_row)

            rows_to_print.extend(self._rows[int(-1 * temp_r) - 1:-1])

        df = pd.DataFrame(rows_to_print)
        result += "\nSome Rows: = \n" + str(df)

        return result

    def _add_row(self, r):
        if self._rows is None:
            self._rows = []
        self._rows.append(r)

    def _load(self):

        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)

        with open(full_name, "r") as txt_file:
            csv_d_rdr = csv.DictReader(txt_file)
            for r in csv_d_rdr:
                self._add_row(r)

        self._logger.debug("CSVDataTable._load: Loaded " + str(len(self._rows)) + " rows")

    def save(self):
        """
        Write the information back to a file.
        :return: None
        """

        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)

        with open(full_name, "w") as txt_file:
            csv_d_wrt = csv.DictWriter(txt_file, fieldnames=self._rows[0].keys())
            csv_d_wrt.writeheader()
            for row in self._rows:
                csv_d_wrt.writerow(row)

    @staticmethod
    def matches_template(row, template):

        result = True
        if template is not None:
            for k, v in template.items():
                if v != row.get(k, None):
                    result = False
                    break

        return result

    @staticmethod
    def matches_key_fields(row, key_columns, key_fields):
        result = True
        if key_fields is not None:
            for i in range(len(key_columns)):
                if key_fields[i] != row.get(key_columns[i], None):
                    result = False
                    break

        return result

    @staticmethod
    def get_columns(row, col_list):
        result = {}
        for c in col_list:
            result[c] = row[c]
        return result

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
            field_list = self._rows[0]

        result = {}
        for row in self._rows:
            if CSVDataTable.matches_key_fields(row, self._data["key_columns"], key_fields):
                result = CSVDataTable.get_columns(row, field_list)

        return result

    def find_by_primary_key_list(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a list of dictionaries containing the requested fields for the record identified
            by the key.

        It can be used to check whether there exist multiple primary keys in the table, which should not be
        allowed.
        """

        if key_fields is None or len(key_fields) is 0:
            return None

        if field_list is None:
            field_list = self._rows[0]

        result = []
        for row in self._rows:
            if CSVDataTable.matches_key_fields(row, self._data["key_columns"], key_fields):
                result.append(CSVDataTable.get_columns(row, field_list))

        return result

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
            field_list = self._rows[0]

        result = []
        for row in self._rows:
            if CSVDataTable.matches_template(row, template):
                result.append(CSVDataTable.get_columns(row, field_list))

        return result

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """

        result = 0

        if key_fields is None or len(key_fields) is 0:
            return result

        for row in self._rows:
            if CSVDataTable.matches_key_fields(row, self._data["key_columns"], key_fields):
                self._rows.remove(row)
                result += 1

        if result > 0:
            CSVDataTable.save(self)

        return result

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """

        result = 0

        if template is None or len(template) is 0:
            return result

        matched_rows = self.find_by_template(template)
        for row in matched_rows:
            self._rows.remove(row)
            result += 1

        if result > 0:
            CSVDataTable.save(self)

        return result

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """

        result = 0

        if key_fields is None or len(key_fields) is 0 or new_values is None or len(new_values) is 0:
            return result

        need_update = 0
        primary_values = []
        tmp_rows = self._rows.copy()
        for row in self._rows:
            if CSVDataTable.matches_key_fields(row, self._data["key_columns"], key_fields):
                for k in new_values.keys():
                    if row[k] != new_values[k]:
                        need_update = 1
                    row[k] = new_values[k]
                if need_update is 1:
                    for key in self._data["key_columns"]:
                        if row[key] not in primary_values:
                            primary_values.append(row[key])
                    result += 1

        if len(self.find_by_primary_key_list(primary_values)) > 1:
            self._rows = tmp_rows
            raise Exception("Error when update: duplicate primary key fields found")

        CSVDataTable.save(self)
        return result

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """

        result = 0

        if template is None or len(template) is 0 or new_values is None or len(new_values) is 0:
            return result

        need_update = 0
        key_fields = []
        tmp_rows = self._rows.copy()
        for row in self._rows:
            if CSVDataTable.matches_template(row, template):
                for k in new_values.keys():
                    if row[k] != new_values[k]:
                        need_update = 1
                    row[k] = new_values[k]
                if need_update is 1:
                    for key in self._data["key_columns"]:
                        key_fields.append(row[key])
                    if len(self.find_by_primary_key_list(key_fields)) > 1:
                        self._rows = tmp_rows
                        raise Exception("Error when update: duplicate primary key fields found")
                    result += 1

        CSVDataTable.save(self)
        return result

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """

        if new_record is None or len(new_record) is 0:
            return

        key_fields = []
        my_keys = self._rows[0].keys()
        for key in new_record.keys():
            if key not in my_keys:
                raise Exception("Error when insert: new record does not match with the key fields.")

        for key in self._data["key_columns"]:
            try:
                key_fields.append(new_record[key])
            except Exception as errMsg:
                raise Exception("Error when insert: primary keys not fully specified.")

        exist = 0
        tmp_rows = self._rows.copy()
        for row in self._rows:
            if CSVDataTable.matches_template(row, new_record):
                exist = 1
        if exist is 0:
            for key in self._rows[0].keys():
                if key not in new_record.keys():
                    new_record[key] = None
            self._rows.append(new_record)

        if len(self.find_by_primary_key_list(key_fields)) > 1:
            self._rows = tmp_rows
            raise Exception("Error when insert duplicate primary key fields found")

        CSVDataTable.save(self)
        return

    def get_rows(self):
        return self._rows
