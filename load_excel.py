import jaydebeapi
import pandas as pd
import configparser
import os
import sys
import tempfile
import zipfile
import warnings
import requests
import json
import time
import socket

version = 20250613


SQL_TYPE_MAPPING = {
    'sqlserver': {
        'int64': 'BIGINT',
        'float64': 'FLOAT',
        'object': 'VARCHAR(4000)',
        'datetime64[ns]': 'DATETIME',
        'bool': 'BIT',
    },
    'oracle': {
        'int64': 'NUMBER',
        'float64': 'FLOAT',
        'object': 'VARCHAR2(4000)',
        'datetime64[ns]': 'DATE',
        'bool': 'NUMBER(1)',
    },
    'snowflake': {
        'int64': 'NUMBER',
        'float64': 'FLOAT',
        'object': 'VARCHAR',
        'datetime64[ns]': 'TIMESTAMP_NTZ',
        'bool': 'BOOLEAN',
    },
    'databricks': {  # Spark SQL data types
        'int64': 'BIGINT',
        'float64': 'DOUBLE',
        'object': 'STRING',
        'datetime64[ns]': 'TIMESTAMP',
        'bool': 'BOOLEAN',
    },
    'mysql': {
        'int64': 'BIGINT',
        'float64': 'DOUBLE',
        'object': 'VARCHAR(255)',
        'datetime64[ns]': 'DATETIME',
        'bool': 'TINYINT(1)',
    },
    'postgres': {
        'int64': 'BIGINT',
        'float64': 'DOUBLE PRECISION',
        'object': 'TEXT',
        'datetime64[ns]': 'TIMESTAMP',
        'bool': 'BOOLEAN',
    },
}

def infer_sql_type(col_series, sql_types_map):
    if col_series.isnull().any():
        return sql_types_map.get('object', 'VARCHAR(255)')
    
    dtype_str = str(col_series.dtype)
    sql_type = sql_types_map.get(dtype_str)
    
    if not sql_type:
        return sql_types_map.get('object', 'VARCHAR(255)')
    
    return sql_type

class DBHandler:
    def __init__(self, db_type, jdbc_url, driver_class, jar_path, user, password, schema=None, truncate_table=False, drop_table=False, debug=False):
        self.db_type = db_type.lower()
        self.schema = schema
        self.truncate_table = truncate_table
        self.drop_table = drop_table
        self.debug = debug
        self.conn = jaydebeapi.connect(driver_class, jdbc_url, [user, password], jar_path)
        self.cursor = self.conn.cursor()
    
    def debug_print(self, statement):
        if self.debug:
            print(f"[DEBUG] Executing SQL:\n{statement}\n")
    
    def format_table_name(self, table_name):
        if self.schema:
            return f'"{self.schema}"."{table_name}"'
        else:
            return f'"{table_name}"'
    
    def quote_identifier(self, identifier):
        return f'"{identifier}"'

    def table_exists(self, table_name):
        schema = self.schema
        table = table_name
        
        schema_quoted = f"'{schema}'" if schema else None
        table_quoted = f"'{table}'"

        if self.db_type == 'sqlserver':
            query = f"""
                SELECT * FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = {schema_quoted} AND TABLE_NAME = {table_quoted}
            """
        elif self.db_type == 'oracle':
            query = f"""
                SELECT * FROM all_tables
                WHERE OWNER = '{schema.upper()}' AND table_name = '{table.upper()}'
            """
        elif self.db_type == 'snowflake':
            query = f"""
                SELECT * FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = '{schema.upper()}' AND TABLE_NAME = {table_quoted}
            """
        elif self.db_type == 'databricks':
            query = f"SHOW TABLES IN {schema}"
            self.debug_print(query)
            self.cursor.execute(query)
            tables = [row[1] for row in self.cursor.fetchall()]
            return table.lower() in [t.lower() for t in tables]
        elif self.db_type == 'mysql':
            query = f"""
                SELECT * FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = {schema_quoted} AND TABLE_NAME = {table_quoted}
            """
        elif self.db_type == 'postgres':
            query = f"""
                SELECT * FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = {schema_quoted} AND TABLE_NAME = {table_quoted}
            """
        else:
            raise NotImplementedError(f"Table check not implemented for {self.db_type}")
        
        if self.db_type != 'databricks':
            self.debug_print(query)
            self.cursor.execute(query)
            return self.cursor.fetchone() is not None

    def drop_table_if_needed(self, table_name):
        if not self.drop_table:
            return
        
        qualified_name = self.format_table_name(table_name)
        drop_sql = f"DROP TABLE {qualified_name}"
        self.debug_print(drop_sql)
        print(f"Dropping table {qualified_name}")
        self.cursor.execute(drop_sql)
        self.conn.commit()

    def create_table(self, table_name, df):
        sql_types = SQL_TYPE_MAPPING[self.db_type]
        columns = []
        for col in df.columns:
            sql_type = infer_sql_type(df[col], sql_types)
            columns.append(f'"{col}" {sql_type}')
        columns_sql = ", ".join(columns)
        qualified_name = self.format_table_name(table_name)
        create_sql = f"CREATE TABLE {qualified_name} ({columns_sql})"
        self.debug_print(create_sql)
        self.cursor.execute(create_sql)
        self.conn.commit()

    def truncate_table_if_needed(self, table_name):
        if self.drop_table:
            return
        if not self.truncate_table:
            return
        qualified_name = self.format_table_name(table_name)
        truncate_sql = ""
        if self.db_type in ['sqlserver', 'mysql', 'postgres', 'oracle', 'snowflake', 'databricks']:
            truncate_sql = f"TRUNCATE TABLE {qualified_name}"
        else:
            raise NotImplementedError(f"Truncate not implemented for {self.db_type}")
        self.debug_print(truncate_sql)
        print(f"Truncating table {qualified_name}")
        self.cursor.execute(truncate_sql)
        self.conn.commit()

    def insert_data(self, table_name, df):
        qualified_name = self.format_table_name(table_name)
        cols = [f'"{col}"' for col in df.columns]
        placeholders = ", ".join(["?"] * len(cols))
        insert_sql = f'INSERT INTO {qualified_name} ({", ".join(cols)}) VALUES ({placeholders})'
        self.debug_print(insert_sql)

        data_tuples = [tuple(row) for row in df.itertuples(index=False, name=None)]
        self.cursor.executemany(insert_sql, data_tuples)
        self.conn.commit()

def process_excel_files(excel_files, db_handler):
    from pandas.errors import ParserWarning

    processed_tables = set()

    for file_path in excel_files:
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            continue

        xls = pd.ExcelFile(file_path)
        for sheet_name in xls.sheet_names:
            df = xls.parse(sheet_name)

            for col in df.columns:
                if df[col].dtype == 'object':
                    try:
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore", UserWarning)
                            df[col] = pd.to_datetime(df[col])
                    except Exception:
                        pass

            sql_types = SQL_TYPE_MAPPING[db_handler.db_type]
            for col in df.columns:
                col_type = infer_sql_type(df[col], sql_types)
                if 'CHAR' in col_type.upper() or 'TEXT' in col_type.upper() or 'VARCHAR' in col_type.upper() or 'STRING' in col_type.upper():
                    df[col] = df[col].apply(lambda x: None if pd.isnull(x) else str(x))
                elif 'DATE' in col_type.upper() or 'TIMESTAMP' in col_type.upper():
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    df[col] = df[col].apply(lambda x: x.isoformat() if pd.notnull(x) else None)
                else:
                    df[col] = df[col].apply(lambda x: None if pd.isnull(x) else x)

            table_name = sheet_name.strip().replace(" ", "_")

            if db_handler.table_exists(table_name):
                if db_handler.drop_table:
                    if table_name not in processed_tables:
                        db_handler.drop_table_if_needed(table_name)
                        processed_tables.add(table_name)
                        print(f"Creating table {table_name} in schema {db_handler.schema}")
                        db_handler.create_table(table_name, df)
                    else:
                        print(f"Appending data to existing table {table_name} in schema {db_handler.schema}")
                else:
                    if db_handler.truncate_table:
                        if table_name not in processed_tables:
                            db_handler.truncate_table_if_needed(table_name)
                            processed_tables.add(table_name)
                        print(f"Appending data to existing table {table_name} in schema {db_handler.schema}")
            else:
                print(f"Creating table {table_name} in schema {db_handler.schema}")
                db_handler.create_table(table_name, df)
                processed_tables.add(table_name)

            print(f"Inserting data into {table_name} in schema {db_handler.schema}")
            db_handler.insert_data(table_name, df)

def get_excel_files_from_input(input_path):
    if input_path.lower().endswith('.zip'):
        temp_dir = tempfile.mkdtemp()
        with zipfile.ZipFile(input_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)

        excel_files = []
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                if file.lower().endswith(('.xls', '.xlsx')):
                    excel_files.append(os.path.join(root, file))

        if not excel_files:
            print("No Excel files found in the zip archive.")
        return excel_files

    elif os.path.isfile(input_path) and input_path.lower().endswith(('.xls', '.xlsx')):
        return [input_path]

    else:
        raise FileNotFoundError(f"Input path {input_path} is not a valid Excel file or zip archive.")

def find_input_file(base_path):
    zip_path = base_path + '.zip'
    xlsx_path = base_path + '.xlsx'

    zip_exists = os.path.isfile(zip_path)
    xlsx_exists = os.path.isfile(xlsx_path)

    if zip_exists and xlsx_exists:
        zip_mtime = os.path.getmtime(zip_path)
        xlsx_mtime = os.path.getmtime(xlsx_path)
        if zip_mtime > xlsx_mtime:
            return zip_path
        else:
            return xlsx_path
    elif zip_exists:
        return zip_path
    elif xlsx_exists:
        return xlsx_path
    else:
        raise FileNotFoundError(f"Neither '{zip_path}' nor '{xlsx_path}' found.")

def infaLog(annotation=""):
    try:
        ## This is simply a "phone home" call.
        ## Just to note which Informatica Org is using this script
        ## If it's unable to reach this URL, it will ignore.
        this_headers = {"Content-Type": "application/json", "X-Auth-Key": "b74a58ca9f170e49f65b7c56df0f452b0861c8c870864599b2fbc656ff758f5d"}
        logs=[{"timestamp": time.time(), "function": f"[{os.path.basename(__file__)}][main]", "execution_time": "N/A", "annotation": annotation, "machine": socket.gethostname()}]
        response=requests.post("https://infa-lic-worker.tim-qin-yujue.workers.dev", data=json.dumps({"logs": logs}), headers=this_headers)
    except:
        pass

if __name__ == "__main__":
    print(f"Running load_excel.py Version {version}")
    config = configparser.ConfigParser()
    config.read('config.ini')

    db_config = config['database']
    app_config = config['app']

    truncate_table_flag = db_config.getboolean('truncate_table', fallback=False)
    drop_table_flag = db_config.getboolean('drop_table', fallback=False)
    debug_flag = app_config.getboolean('debug', fallback=False)

    
    db_handler = DBHandler(
        db_type=db_config.get('db_type'),
        jdbc_url=db_config.get('jdbc_url'),
        driver_class=db_config.get('driver_class'),
        jar_path=db_config.get('jar_path'),
        user=db_config.get('user'),
        password=db_config.get('password'),
        schema=db_config.get('schema'),
        truncate_table=truncate_table_flag,
        drop_table=drop_table_flag,
        debug=debug_flag,
    )

    # Command-line argument overrides config file input_path
    if len(sys.argv) > 1:
        input_path = sys.argv[1]
        if not os.path.isfile(input_path):
            raise FileNotFoundError(f"Input file '{input_path}' does not exist.")
    else:
        base_path = app_config.get('input_path')
        if not base_path:
            raise Exception("No input_path specified in config and no command-line argument given.")
        input_path = find_input_file(base_path)
    infaLog(f"DB_type: {db_config.get('db_type')}, Input: {input_path}")
    excel_files = get_excel_files_from_input(input_path)
    process_excel_files(excel_files, db_handler)
