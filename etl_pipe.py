
import pandas as pd
import pyodbc

class SpotifyETL:
    def __init__(self,json_path,table_name,server_name,database_name):
        self.json_path = json_path
        self.table_name = table_name
        self.server_name = server_name
        self.database_name = database_name
        self.df = None

    def extract(self):
        try:
            self.df = pd.read_json(self.json_path)
            print("Data extraction successful.")
        except Exception as e:
            print(f"Error during data extraction: {e}")
            raise
        
    def transform(self):
        try:
            if 'endTime' in self.df.columns:
                self.df.rename(columns={
                    'endTime': 'end_time',
                    'artistName': 'artist_name',
                    'trackName': 'track_name',
                    'msPlayed': 'ms_played'
                }, inplace=True)

                self.df['end_time'] = pd.to_datetime(self.df['end_time'])
                self.df['minutes_played'] = round(self.df['ms_played'] / 60000, 2)
                self.df = self.df[self.df['ms_played'] > 30000] # filter out tracks played for less than 30 seconds
        except Exception as e:
            print(f"Error during data transformation: {e}")
            raise

    def load(self):
        try:
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.server_name};DATABASE={self.database_name};Trusted_Connection=yes;"
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()

            # Create table if it doesn't exist
            create_table_query = f"""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{self.table_name}')
            CREATE TABLE {self.table_name} (
                end_time DATETIME,
                artist_name NVARCHAR(255),
                track_name NVARCHAR(255),
                ms_played INT,
                minutes_played FLOAT
            )
            """
            cursor.execute(create_table_query)
            conn.commit()

            # Insert data into the table
            for index, row in self.df.iterrows():
                insert_query = f"""
                INSERT INTO {self.table_name} (end_time, artist_name, track_name, ms_played, minutes_played)
                VALUES (?, ?, ?, ?, ?)
                """
                cursor.execute(insert_query, (row['end_time'], row['artist_name'], row['track_name'], row['ms_played'], row['minutes_played']))
            
            conn.commit()
            print("Data loading successful.")
        except Exception as e:
            print(f"Error during data loading: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def run(self):
        print("========== ETL SURECI BASLADI ==========")
        self.extract()
        self.transform()
        self.load()
        print("========== ETL SURECI TAMAMLANDI ==========")