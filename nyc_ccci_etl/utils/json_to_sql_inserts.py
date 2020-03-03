import json

def json_to_sql_inserts(table_name, json):
    sql_string = ""
    for i, record_dict in enumerate(json):
        columns = list(json[i].keys())
        sql_string += 'INSERT INTO {} '.format( table_name )
        sql_string += "(" + ', '.join(columns) + ")\nVALUES "
        values = []
        for col_names, val in record_dict.items():
            if type(val) == str:
                val = val.replace("'", "''")
                val = "'" + val + "'"
            values += [ str(val) ]
        sql_string += "(" + ', '.join(values) + "),\n"
        sql_string = sql_string[:-2] + ";"
    return sql_string
    