# import mysql.connector
# from mysql.connector import Error

# def create_connection():
#     try:
#         connection = mysql.connector.connect(
#             host='172.105.61.104',
#             user='root',
#             password='MahitNahi@12',  # Replace with your actual password
#             database='stocksync'  # Specify the database name
#         )
#         if connection.is_connected():
#             print('Connected to MySQL database')
#             return connection
#     except Error as e:
#         print(f'Error: {e}')
#         return None

# def query_users_table(connection):
#     try:
#         cursor = connection.cursor()
#         cursor.execute("SELECT * FROM user;")  # Query to select all records from the users table
#         rows = cursor.fetchall()
#         print('Users Table:')
#         for row in rows:
#             print(row)
#     except Error as e:
#         print(f'Error: {e}')
#     finally:
#         cursor.close()

# # Example usage
# conn = create_connection()
# if conn:
#     query_users_table(conn)
#     conn.close()
