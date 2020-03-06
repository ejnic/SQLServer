import pyodbc
import pandas as pd
import sqlalchemy as sa

engine = sa.create_engine('mssql+pyodbc://dev1.whedb.iu.edu/UAGrad_eDocs?driver=SQL+Server+Native+Client+11.0')
# query = 'SELECT [EmployeeID] [FirstName], [LastName], [ManagerID] FROM Employee;'
# df = pd.read_sql_query(query, engine)


df = pd.read_csv('N:\\eApp\\Liaison\\indiana_users.csv', skiprows=1, names = ['campus','first_name','last_name','email','phone_number','extension','primary','is_active','programname','webadmitname','roles','users_created_at','users_created_at2','last_login_at','login_count'])

df.replace({r'[^\x00-\x7F]+':''}, regex=True, inplace=True)
#trim to columns needed
users = df.iloc[:,[0,1,2,3,7,8,9,10,11,13,14]]

#parse email to networkID, fix column names
emailparse = users["email"].str.split("@", n = 2, expand = True)
users = users.join(emailparse)
users = users.drop(1, axis=1)
users.rename(columns = {0:'networkid'}, inplace = True)
users['webadmitname'] = users['webadmitname'].str.lower()

users.to_sql('wausers', engine)

''' users.to_sql('wausers', engine, 'UAGrad_eDocs', if_exists='replace', chunksize=1000,
                 dtype=
                 {'campus':sa.types.VARCHAR(df.campus.str.len().max()),
                 'first_name':sa.types.VARCHAR(df.first_name.str.len().max()),
                 'last_name':sa.types.VARCHAR(df.last_name.str.len().max()),
                 'email':sa.types.VARCHAR(df.email.str.len().max()),
                 'programname':sa.types.VARCHAR(df.programname.str.len().max()),
                 'webadmitname':sa.types.VARCHAR(df.webadmitname.str.len().max()),
                 'roles':sa.types.VARCHAR(df.roles.str.len().max()),
                 'networkid':sa.types.VARCHAR(25),
                 'is_active':sa.types.VARCHAR(2),
                 'users_created_at':sa.types.VARCHAR(50),
                 'users_created_at2':sa.types.VARCHAR(50),
                 'last_login_at':sa.types.VARCHAR(50),
                 'login_count':sa.types.VARCHAR(25)
                 })
'''

''' this works
sql_conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server}; SERVER=dev1.whedb.iu.edu; DATABASE=UAGrad_eDocs;   Trusted_Connection=yes')

query = "SELECT [EmployeeID] [FirstName], [LastName], [ManagerID] FROM Employee;"
df = pd.read_sql(query, sql_conn) '''

print(df.head(7))