===== RDBDataTable =====

The databse is loaded through the use of mysql, and the connection info and table info
are passed in through input parameters. Then, through the "run_q" function, SQL queries
are executed in MySQL, and the corresponding outputs can be generated.

Return Values: Exception will be raised if the SQL queries cannot be successfully executed. 
The selected rows are returned in the "find" functions, the number of rows affected are 
returned in "delete" and "update" functions, and nothing is returned in the "insert" function.

- find_by_key & find_by_template: 
These two functions requires the use of "SELECT" and "WHERE" methods to macth correct rows 
in the database and return the selected ones. Find_by_key firslt joins the desired key
values, and uses "WHERE" to combine the primary keys with the values. Find_by_template
converts the lists of primary keys and values into one SQL query. Finally, through "SELECT"
the rows that match the "WHERE" clause will be selected and returned. If "field_list" is not
specified, it is regarded as "*", which would return the whole row. Otherwise, only the 
specified fields will be returned.

- delete_by_key & delete_by_template:
Similar as above, these two functions need to firstly construct the "WHERE" clauses from
the primary key values or from the templates. Then, the selected rows will be removed from
the table through "DELETE FROM [table name]".

- update_by_key & update_by_template:
These two functions require the use of "UPDATE" and "SET" clauses to specify which table
that needs to be updated, and which values should be updated. Then, after the desired rows
are found through "WHERE" clause, they can be updated by the new values. If their values
are not changed, no rows will be updated. And if the new values have changed the primary key 
fields such that there exist duplicated primary keys, the update requests cannot be executed successfully. An exception will be raised in this condition.

- insert:
This function uses the "INSERT INTO [table name]" query to add new rows to the table, as
long as the new row will not lead to duplicate primary keys, or have columns that do not
match with the table structure. Otherwise, insertion cannot be executed successfully, and
an exception will be raised.


===== CSVDataTable =====

The CSV files are loaded through python "open" and "dictreader", and are saved through python
"open" and "dictwritter". The table names and connect info are passed in through the input
parameters

Return Values: The Exception cases for CSVTable are similar to those of the RDBTable. The 
selected rows are returned in the "find" functions, the number of rows affected (in string 
format) are returned in "delete" and "update" functions, and nothing is returned in the 
"insert" function.

- find_by_key & find_by_template: 
Theses two functions match the primary key fields or match the template to find the desired
row(s) in the CSV file.

- delete_by_key & delete_by_template:
These two functions remove the matched rows from the original CSV file. Thus, they require 
the function "save()" when there exist matched rows to be deleted.

- update_by_key & update_by_template:
These two functions update the matched rows from the original CSV file with the new values. 
Thus, they require the function "save()" when there exist matched rows to be updated, and
the new values are valid and will not lead to duplicate primary key values.

- insert:
This function inserts a new row to the table if the new row has valid forms and will noe lead
to duplicated primary key values. To insert, the new row is appended to the original rows, 
and "save()" is used to save the changes.
