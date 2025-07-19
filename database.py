import pyodbc

def get_connection():
    conn = pyodbc.connect(
        r'DRIVER={ODBC Driver 17 for SQL Server};'
        r'SERVER=seeddb.cd74dc0y58xw.sa-east-1.rds.amazonaws.com;'
        r'DATABASE=Seed_CFG_Analytics;'
        r'UID=seed;'
        r'PWD=y6w8f1371Mhc8jP6SW7J30E3qF061p'
    )
    return conn