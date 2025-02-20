import pyodbc
import os
import re

"""
DISCLAIMER:
Although the author is a Microsoft employee, this script is provided as-is, without any official 
support or endorsement from Microsoft. Use it at your own risk.

PURPOSE:
This Python script helps create and drop statistics for Parquet or Delta tables in Synapse Serverless SQL pool.

DETAILS:
- To view table schema, this code connects to Serverless SQL pool and runs the `sp_describe_first_result_set` stored procedure.
- It does NOT create statistics or make changes to your Serverless SQL pool.

OUTPUT:
Generates two files:
- create_stats_openrowset.txt
- drop_stats_openrowset.txt
You can review and execute these files in your Synapse SQL, preferably using SSMS or Azure Data Studio.

VARIABLES TO SET BEFORE RUNNING:
- SERVER_NAME: your_server_name-ondemand.sql.azuresynapse.net
- DATABASE_NAME: The target database in Synapse.
"""

# Configuration variables
SERVER_NAME = 'your_server_name-ondemand.sql.azuresynapse.net'  # Update this with your Serverless SQL endpoint
DATABASE_NAME = 'your_database_name'                            # Update this to your database name
#USER_NAME = ''                                             # Required when using SQL authentication
#PASSWORD = ''                                              # Required when using SQL authentication

# Output file root paths
CREATE_STATS_FILE_PATH_ROOT = r'c:\ss\temp\ss'
DROP_STATS_FILE_PATH_ROOT = r'c:\ss\temp\ss'

def ensure_output_directory(file_path):
    """Ensures that the output directory exists."""
    output_dir = os.path.dirname(file_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

def get_column_names(cursor):
    """Fetches the 'name' column from the sp_describe_first_result_set output."""
    columns = [column[0] for column in cursor.description]
    name_index = columns.index('name')

    rows = cursor.fetchall()
    column_names = [row[name_index] for row in rows]
    return column_names


def write_create_statistics_commands(column_names, openrowset_statement, file_path):
    """
    Writes the CREATE STATISTICS commands to the output file.
    """
    with open(file_path, 'w') as f:
        for column_name in column_names:
            # PRINT statement
            f.write(f"PRINT 'Creating stats for column [{column_name}]...'\n")
            f.write("GO\n")

            # CREATE statement
            create_cmd = (
                "EXEC sys.sp_create_openrowset_statistics N'SELECT [{0}] "
                "FROM {1} AS [q1]';"
            ).format(column_name, openrowset_statement)
            f.write(create_cmd + "\n")
            f.write("GO\n\n")


def write_drop_statistics_commands(column_names, openrowset_statement, file_path):
    """
    Writes the DROP STATISTICS commands to the output file.
    """
    with open(file_path, 'w') as f:
        for column_name in column_names:
            # PRINT statement
            f.write(f"PRINT 'Dropping stats for column [{column_name}]...'\n")
            f.write("GO\n")

            # DROP statement
            drop_cmd = (
                "EXEC sys.sp_drop_openrowset_statistics N'SELECT [{0}] "
                "FROM {1} AS [q1]';"
            ).format(column_name, openrowset_statement)
            f.write(drop_cmd + "\n")
            f.write("GO\n\n")


def get_openrowset_string(view_name, file_path):

    # Regular expression to extract the OPENROWSET statement
    pattern = r"OPENROWSET\([\s\S]*?\)"

    # Search for the pattern in the input string
    match = re.search(pattern, file_path)

    # Extracted OPENROWSET statement
    openrowset_statement_temp = match.group(0) if match else None
    
    openrowset_statement = openrowset_statement_temp.replace("'","''")

    get_column(openrowset_statement, view_name)

    # print(openrowset_statement)
    # return openrowset_statement

def get_column(openrowset_statement, view_name):

    # Concate output file full paths
    CREATE_STATS_FILE_PATH = CREATE_STATS_FILE_PATH_ROOT + '\\' + view_name + '\create_openrowset_stats.txt'
    DROP_STATS_FILE_PATH = DROP_STATS_FILE_PATH_ROOT + '\\' + view_name + '\drop_openrowset_stats.txt'

    # Ensure the output directories exist
    ensure_output_directory(CREATE_STATS_FILE_PATH)
    ensure_output_directory(DROP_STATS_FILE_PATH)

    # Connection string for Azure Synapse Serverless (in this case using AAD + MFA)
    conn_str = (
        'Driver={ODBC Driver 17 for SQL Server};'
        f'Server={SERVER_NAME};'
        f'Database={DATABASE_NAME};'
        #f'UID={USER_NAME};'        #Uncomment this if you want to use SQL authentication
        #f'PWD={PASSWORD}'          #Uncomment this if you want to use SQL authentication
        'Authentication=ActiveDirectoryInteractive;' 
    )

    try:
        # Connect to the Synapse SQL Serverless pool
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Check the table schema
        command = (f"EXEC sp_describe_first_result_set N'SELECT * FROM {openrowset_statement} AS [q1]'")
        print(command)

        cursor.execute(command)

        # Get column names
        column_names = get_column_names(cursor)

        # Write CREATE statistics commands
        write_create_statistics_commands(column_names, openrowset_statement, CREATE_STATS_FILE_PATH)
        print(f"Create statistics commands written to {CREATE_STATS_FILE_PATH}")

        # Write DROP statistics commands
        write_drop_statistics_commands(column_names, openrowset_statement, DROP_STATS_FILE_PATH)
        print(f"Drop statistics commands written to {DROP_STATS_FILE_PATH}")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the connection
        if 'conn' in locals():
            conn.close()


def main():

    # Connection string for Azure Synapse Serverless (in this case using AAD + MFA)
    conn_str = (
        'Driver={ODBC Driver 17 for SQL Server};'
        f'Server={SERVER_NAME};'
        f'Database={DATABASE_NAME};'
        f'UID={USER_NAME};'
        f'PWD={PASSWORD}'
        # 'Authentication=ActiveDirectoryInteractive;' 
    )

    try:
        # Connect to the Synapse SQL Serverless pool
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Get view definition
        command = ("select s.name as SchemaName, o.name as ViewName, definition from sys.objects o join sys.sql_modules m on m.object_id = o.object_id join sys.schemas s on o.schema_id = s.schema_id where o.type = 'V'")

        cursor.execute(command)

        # Fetch all results 
        rows = cursor.fetchall()
        # print(rows)

        for row in rows:
            print(row)
            openrowset_test = row.definition
            index = openrowset_test.find('OPENROWSET')
            if index >= 0:
                view_name = row.SchemaName + '.' + row.ViewName
                get_openrowset_string(view_name, row.definition)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the connection
        if 'conn' in locals():
            conn.close()


if __name__ == '__main__':
    main()
