import pandas as pd
from sqlalchemy import create_engine, select
import json
from itertools import cycle
from sqlalchemy import create_engine, select, text
username = 'root'
password = 'Yzy4321326...'
host = 'localhost'
database = 'Schema1'
engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}/{database}?charset=utf8mb4')
df = pd.read_csv('office_expenses.csv')
office_data = df[['Office Name', 'Address', 'State', 'City']].drop_duplicates()
office_data['OfficeID'] = range(1, len(office_data) + 1)
with engine.connect() as connection:
    for _, row in office_data.iterrows():
        query = select(text('1')).select_from(text('Offices')).where(text("Address = :address"))
        result = connection.execute(query, {'address': row['Address']}).fetchone()
        exists = result is not None
        if not exists:
            print( row['Address'])
            insert_statement = text("INSERT INTO Offices (OfficeID, Address, City, State) VALUES (:office_id, :address, :city, :state)")
            params = {
                'office_id': row['OfficeID'],
                'address': row['Address'],
                'city': row['City'],
                'state': row['State']
            }
            connection.execute(insert_statement, params)

office_ids = pd.read_sql("SELECT OfficeID, Address FROM Offices", con=engine)
print(office_ids)
df = df.merge(office_ids, how='left', on='Address')
print(df)
expense_data = df[['OfficeID', 'Date', 'Expense Type', 'Amount']]
with engine.connect() as connection:
    for _, row in df.iterrows():
        insert_statement = text("INSERT INTO Expenses (OfficeID, ExpenseDate, ExpenseType, Amount) VALUES (:office_id, :date, :expense_type, :amount)")
        params = {
            'office_id': row['OfficeID'],
            'date': row['Date'],
            'expense_type': row['Expense Type'],
            'amount': row['Amount']
        }
        connection.execute(insert_statement, params)
print("Transforming success")
df_offices = pd.read_csv('office_expenses.csv')
df_employees = pd.read_csv('employees.csv')
office_data = df_offices[['Office Name', 'Address', 'State', 'City']].drop_duplicates()
with engine.connect() as connection:
    for _, row in office_data.iterrows():
        query = select(text('1')).select_from(text('Offices')).where(text("Address = :address"))
        result = connection.execute(query, {'address': row['Address']}).fetchone()
        exists = result is not None
        if not exists:
            print(row['Address'])
            insert_statement = text(
                "INSERT INTO Offices (OfficeID, Address, City, State) VALUES (:office_id, :address, :city, :state)")
            params = {
                'office_id': row['OfficeID'],
                'address': row['Address'],
                'city': row['City'],
                'state': row['State']
            }
            connection.execute(insert_statement, params)
office_ids = pd.read_sql("SELECT OfficeID, Address FROM Offices", con=engine)
df_offices = df_offices.merge(office_ids, how='left', on='Address')
df_employees[['LastName', 'FirstName']] = df_employees['Full Name'].str.split(',', expand=True)
df_employees['FirstName'] = df_employees['FirstName'].str.strip()
df_employees['LastName'] = df_employees['LastName'].str.strip()
df_employees = df_employees.merge(office_ids, left_on='Office Name', right_on='Address', how='left')
employee_data = df_employees[['FirstName', 'LastName', 'Email', 'Base Salary', 'OfficeID']]
with engine.connect() as connection:
    for _, row in employee_data.iterrows():
        insert_statement = text("""
            INSERT INTO Employees (FirstName, LastName, Email, Salaries, OfficeID) 
            VALUES (:first_name, :last_name, :email, :salary, :office_id)
        """)
        params = {
            'first_name': row['FirstName'],
            'last_name': row['LastName'],
            'email': row['Email'],
            'salary': row['Base Salary'],
            'office_id': row['OfficeID']
        }
        connection.execute(insert_statement, params)
df_offices = pd.read_csv('office_expenses.csv')
df_employees = pd.read_csv('employees.csv')
df_clients = pd.read_csv('clients.csv')
office_data = df_offices[['Office Name', 'Address', 'State', 'City']].drop_duplicates()
with engine.connect() as connection:
    for _, row in office_data.iterrows():
        query = select(text('1')).select_from(text('Offices')).where(text("Address = :address"))
        result = connection.execute(query, {'address': row['Address']}).fetchone()
        exists = result is not None
        if not exists:
            print(row['Address'])
            insert_statement = text(
                "INSERT INTO Offices (OfficeID, Address, City, State) VALUES (:office_id, :address, :city, :state)")
            params = {
                'office_id': row['OfficeID'],
                'address': row['Address'],
                'city': row['City'],
                'state': row['State']
            }
            connection.execute(insert_statement, params)
office_ids = pd.read_sql("SELECT OfficeID, Address FROM Offices", con=engine)
df_offices = df_offices.merge(office_ids, how='left', on='Address')
df_employees[['LastName', 'FirstName']] = df_employees['Full Name'].str.split(',', expand=True)
df_employees['FirstName'] = df_employees['FirstName'].str.strip()
df_employees['LastName'] = df_employees['LastName'].str.strip()
df_employees = df_employees.merge(office_ids, left_on='Office Name', right_on='Address', how='left')
employee_data = df_employees[['FirstName', 'LastName', 'Email', 'Base Salary', 'OfficeID']]
with engine.connect() as connection:
    for _, row in employee_data.iterrows():
        insert_statement = text("""
            INSERT INTO Employees (FirstName, LastName, Email, Salaries, OfficeID) 
            VALUES (:first_name, :last_name, :email, :salary, :office_id)
        """)
        params = {
            'first_name': row['FirstName'],
            'last_name': row['LastName'],
            'email': row['Email'],
            'salary': row['Base Salary'],
            'office_id': row['OfficeID']
        }
        connection.execute(insert_statement, params)
df_clients[['LastName', 'FirstName']] = df_clients['Name'].str.split(',', expand=True)
df_clients['FirstName'] = df_clients['FirstName'].str.strip()
df_clients['LastName'] = df_clients['LastName'].str.strip()
client_data = df_clients[['FirstName', 'LastName', 'Email', 'Phone Number']]
with engine.connect() as connection:
    for _, row in client_data.iterrows():
        insert_statement = text("""
            INSERT INTO Clients (FirstName, LastName, Email, Phone, ContactPreference) 
            VALUES (:first_name, :last_name, :email, :phone, 'Email')
        """)
        params = {
            'first_name': row['FirstName'],
            'last_name': row['LastName'],
            'email': row['Email'],
            'phone': row['Phone Number']
        }
        connection.execute(insert_statement, params)
df_home_listings = pd.read_csv('home_listings.csv')
home_type_data = df_home_listings[['Home Type']].drop_duplicates()
with engine.connect() as connection:
    for _, row in home_type_data.iterrows():
        exists_query = select([1]).select_from(text('HomeTypes')).where(text('TypeName = :type_name'))
        exists = connection.execute(exists_query, {'type_name': row['Home Type']}).fetchone()
        if not exists:
            insert_statement = text("INSERT INTO HomeTypes (TypeName) VALUES (:type_name)")
            connection.execute(insert_statement, {'type_name': row['Home Type']})
type_ids = pd.read_sql("SELECT TypeID, TypeName FROM HomeTypes", con=engine)
df_home_listings = df_home_listings.merge(type_ids, how='left', on='Home Type')
df_transactions = pd.read_csv('transactions.csv')
status_data = df_transactions[['Status']].drop_duplicates()
with engine.connect() as connection:
    for _, row in status_data.iterrows():
        exists_query = select([1]).select_from(text('Availability')).where(text('StatusName = :status_name'))
        exists = connection.execute(exists_query, {'status_name': row['Status']}).fetchone()
        if not exists:
            insert_statement = text("INSERT INTO Availability (StatusName) VALUES (:status_name)")
            connection.execute(insert_statement, {'status_name': row['Status']})
status_ids = pd.read_sql("SELECT StatusID, StatusName FROM Availability", con=engine)
df_transactions = df_transactions.merge(status_ids, how='left', on='Status')
homes_data = df_home_listings[['Street', 'City', 'State', 'TypeID']]
homes_data = homes_data.rename(columns={'Street': 'Address'})
with engine.connect() as connection:
    for _, row in homes_data.iterrows():
        insert_statement = text("""
            INSERT INTO Homes (Address, City, State, Type) 
            VALUES (:address, :city, :state, :type_id)
        """)
        params = {
            'address': row['Address'],
            'city': row['City'],
            'state': row['State'],
            'type_id': row['TypeID']
        }
        connection.execute(insert_statement, params)
with engine.connect() as connection:
    employee_ids = [row[0] for row in connection.execute(
        text("SELECT EmployeeID FROM Employees")).fetchall()]
    home_ids = [row[0] for row in connection.execute(
        text("SELECT HomeID FROM Homes")).fetchall()]
    client_ids = [row[0] for row in connection.execute(
        text("SELECT ClientID FROM Clients")).fetchall()]
    employee_cycle = cycle(employee_ids)
    home_cycle = cycle(home_ids)
    client_cycle = cycle(client_ids)
    for home_id, client_id in zip(home_cycle, client_cycle):
        current_employee_id = next(employee_cycle)
        insert_statement = text("""
            INSERT INTO Managing (EmployeeID, HomeID, ClientID) 
            VALUES (:employee_id, :home_id, :client_id)
        """)
        params = {
            'employee_id': current_employee_id,
            'home_id': home_id,
            'client_id': client_id
        }
        connection.execute(insert_statement, params)
df_home_listings = pd.read_csv('home_listings.csv')
def extract_highest_rating(schools_info):
    try:
        schools = json.loads(schools_info.replace("'", '"'))
        highest_rating = max(school.get('rating', 0) for school in schools)
    except Exception as e:
        print(f"Error processing schools info: {e}")
        highest_rating = 0
    return highest_rating
df_home_listings['HighestRating'] = df_home_listings['Nearby Schools'].apply(extract_highest_rating)
df_home_listings['FeatureName'] = df_home_listings.apply(lambda row: f"{row['Bedrooms']},{row['Bathrooms']},{row['Square Footage']},{row['HighestRating']}", axis=1)
with engine.connect() as connection:
    for feature_name in df_home_listings['FeatureName'].unique():
        insert_statement = text("INSERT INTO Features (FeatureName) VALUES (:feature_name)")
        connection.execute(insert_statement, {'feature_name': feature_name})
df_home_listings = pd.read_csv('home_listings.csv')
def extract_highest_rating(schools_info):
    try:
        schools = json.loads(schools_info.replace("'", '"'))
        highest_rating = max(school.get('rating', 0) for school in schools)
    except Exception as e:
        highest_rating = 0
    return highest_rating
df_home_listings['HighestRating'] = df_home_listings['Nearby Schools'].apply(extract_highest_rating)
df_home_listings['FeatureName'] = df_home_listings.apply(
    lambda row: f"{row['Bedrooms']},{row['Bathrooms']},{row['Square Footage']},{row['HighestRating']}", axis=1)
with engine.connect() as connection:
    for _, row in df_home_listings.iterrows():
        exists_query = text("SELECT FeatureID FROM Features WHERE FeatureName = :feature_name")
        exists = connection.execute(exists_query, {'feature_name': row['FeatureName']}).fetchone()
        if not exists:
            insert_feature = text("INSERT INTO Features (FeatureName) VALUES (:feature_name)")
            connection.execute(insert_feature, {'feature_name': row['FeatureName']})
            feature_id = connection.execute(text("SELECT LAST_INSERT_ID()")).fetchone()[0]
        else:
            feature_id = exists[0]
        home_id_query = text("SELECT HomeID FROM Homes WHERE Address = :address")
        home_id = connection.execute(home_id_query, {'address': row['Address']}).fetchone()[0]
        insert_home_feature = text("INSERT INTO Home_Features (HomeID, FeatureID) VALUES (:home_id, :feature_id)")
        connection.execute(insert_home_feature, {'home_id': home_id, 'feature_id': feature_id})
df_clients = pd.read_csv('clients.csv')
df_home_types = pd.read_sql("SELECT * FROM HomeTypes", con=engine)
df_home_listings = pd.read_csv('home_listings.csv')
df_clients = df_clients.merge(df_home_types, left_on='Preferred Home Type', right_on='TypeName', how='left')
price_stats = df_home_listings.groupby('Home Type').agg({'Listed Price': ['min', 'max']}).reset_index()
price_stats.columns = ['Home Type', 'MinPrice', 'MaxPrice']
df_clients = df_clients.merge(price_stats, left_on='Preferred Home Type', right_on='Home Type', how='left')
with engine.connect() as connection:
    for _, row in df_clients.iterrows():
        feature_string = f"{row['Preferred Number of Bedrooms']},{row['Preferred Number of Bathrooms']},{row['Preferred Square Footage']},{row['Preferred School Rating']}"
        insert_statement = text("""
            INSERT INTO Client_Preferences 
                (ClientID, TypeID, MinPrice, MaxPrice, Features) 
            VALUES (:client_id, :type_id, :min_price, :max_price, :features)
        """)
        params = {
            'client_id': row['ClientID'],
            'type_id': row['TypeID'],
            'min_price': row['MinPrice'],
            'max_price': row['MaxPrice'],
            'features': feature_string
        }
        connection.execute(insert_statement, params)
df_clients_db = pd.read_sql("SELECT ClientID, Email FROM Clients", con=engine)
df_clients_csv = pd.read_csv('clients.csv')
df_employees = pd.read_sql("SELECT EmployeeID, FirstName, LastName FROM Employees", con=engine)
df_merged_clients = pd.merge(df_clients_db, df_clients_csv, on='Email')
df_merged_clients[['AssignedFirstName', 'AssignedLastName']] = df_merged_clients['Assigned Employee'].str.split(',', expand=True)
df_merged_clients['AssignedFirstName'] = df_merged_clients['AssignedFirstName'].str.strip()
df_merged_clients['AssignedLastName'] = df_merged_clients['AssignedLastName'].str.strip()
df_final = pd.merge(df_merged_clients, df_employees, left_on=['AssignedFirstName', 'AssignedLastName'], right_on=['FirstName', 'LastName'])
with engine.connect() as connection:
    insert_statement = text("""
                INSERT INTO Client_Agent 
                    (ClientID, AgentID, AssignmentDate) 
                VALUES (:client_id, :agent_id, CURDATE())
            """)
    params = {
        'client_id': row['ClientID'],
        'agent_id': row['EmployeeID']
    }
    connection.execute(insert_statement, params)
df_client_agent = pd.read_sql("SELECT ClientID, AgentID FROM Client_Agent", con=engine)
df_client_agent['AppointmentDate'] = pd.Timestamp('today').normalize() + pd.Timedelta(days=7)
df_client_agent['Purpose'] = 'Initial Meeting'
with engine.connect() as connection:
    for _, row in df_client_agent.iterrows():
        insert_statement = text("""
            INSERT INTO Appointments 
                (ClientID, AgentID, AppointmentDate, Purpose) 
            VALUES (:client_id, :agent_id, :appointment_date, :purpose)
        """)
        params = {
            'client_id': row['ClientID'],
            'agent_id': row['AgentID'],
            'appointment_date': row['AppointmentDate'],
            'purpose': row['Purpose']
        }
        connection.execute(insert_statement, params)
