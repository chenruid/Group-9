import mysql.connector
from mysql.connector import Error

def create_connection(host_name, user_name, user_password):
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("Connection to MySQL server successful")
        return connection
    except Error as e:
        print(f"The error '{e}' occurred")
        return None

def create_database(connection, database_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{database_name}`")
        print("Database created successfully")
    except Error as e:
        print(f"The error '{e}' occurred when creating the database")

def create_database_connection(host_name, user_name, user_password, db_name):
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
        return connection
    except Error as e:
        print(f"The error '{e}' occurred")
        return None
def check_table_exists(cursor, table_name):
    cursor.execute(f"SHOW TABLES LIKE '{table_name}';")
    result = cursor.fetchone()
    return result is not None

def execute_sql_from_file(connection, file_path):
    try:
        with open(file_path, 'r') as file:
            sql_script = file.read()
        commands = [cmd.strip() + ';' for cmd in sql_script.split(';') if cmd.strip()]
        table_creation_order = [
            "HomeTypes", "Availability", "Offices", "Employees", "Clients", "Homes",
            "Managing", "Transactions", "Client_Agent", "Appointments", "Open_Houses",
            "Services", "Service_Appointments"
        ]

        command_priorities = []
        for command in commands:
            highest_priority = -1
            for index, table_name in enumerate(table_creation_order):
                if table_name in command:
                    if index > highest_priority:
                        highest_priority = index
            command_priorities.append((command, highest_priority))
        command_priorities.sort(key=lambda x: x[1])
        sorted_commands = [cmd[0] for cmd in command_priorities]
        for cmd, prio in command_priorities:
            print(f"Priority: {prio}, Command: {cmd}")
        cursor = connection.cursor()
        for command in sorted_commands:
            print(command)
            print(f"Executing command: {command}")
            if "CREATE TABLE" in command and "IF NOT EXISTS" not in command:
                command = command.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
            cursor.execute(command)
            connection.commit()
            if cursor.rowcount == -1:
                print("No rows affected, check SQL command for issues.")
            else:
                print("Command executed successfully")
    except IOError:
        print("File not found")
    except Error as e:
        print(f"The error '{e}' occurred when reading the file: {command}")
def main():
    host = "localhost"
    user = "root"
    password = "Yzy4321326..."  # replace with your own
    database_name = "Schema1"  # replace with your own
    server_connection = create_connection(host, user, password)
    if server_connection:
        create_database(server_connection, database_name)
        server_connection.close()
    db_connection = create_database_connection(host, user, password, database_name)
    if db_connection:
        sql_file_path = "Schema.sql"
        execute_sql_from_file(db_connection, sql_file_path)
        db_connection.close()
if __name__ == '__main__':
    main()
