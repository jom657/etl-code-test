import pandas as pd
import sqlite3
import logging
import os

class ETLProcessor:
    
    def __init__(self, dir):
        log_file='etl_log.log'
        db_file='etl_database.db'
        self.dir = dir
        db_dir = os.path.join(self.dir,'database')
        self.db_file = os.path.join(db_dir,db_file)
        self.log_file = log_file
        self.setup_logging()
        self.load_data()
        
    def setup_logging(self):
        logging.basicConfig(filename=self.log_file, level=logging.INFO, 
                            format='%(asctime)s:%(levelname)s:%(message)s')
        logging.info('Initialized ETLProcessor.')
    
    def load_data(self):
        try:
            raw_files_dir = os.path.join(self.dir,'data')
            self.users_df = pd.read_csv(os.path.join(raw_files_dir, 'users-1.csv'))
            self.transactions_df = pd.read_csv(os.path.join(raw_files_dir, 'transactions-1.csv'))
            self.pricing_df = pd.read_csv(os.path.join(raw_files_dir, 'pricing-1.csv'))
            logging.info(f"Loaded {len(self.users_df)} records from users-1.csv.")
            logging.info(f"Loaded {len(self.transactions_df)} records from transactions-1.csv.")
            logging.info(f"Loaded {len(self.pricing_df)} records from pricing-1.csv.")
        except Exception as e:
            logging.error(f"Error loading data: {e}")
            raise
    
    def explore_data(self):
        # Display the first few rows of the dataframe
        logging.info("First few rows of the transactions dataframe:")
        logging.info(self.transactions_df.head())

        # Display data types of each column
        logging.info("\nData types of each column:")
        logging.info(self.transactions_df.dtypes)

        # Identify missing values in each column
        logging.info("\nMissing values in each column:")
        logging.info(self.transactions_df.isnull().sum())

        # Get summary statistics for numeric columns
        logging.info("\nSummary statistics for numeric columns:")
        logging.info(self.transactions_df.describe())

        # Get unique value counts for categorical columns
        logging.info("\nUnique value counts for categorical columns:")
        logging.info(self.transactions_df.nunique())

        # Check for potential data quality issues in the 'amount' column
        logging.info("\nPotential data quality issues in the 'amount' column:")
        self.transactions_df['amount_numeric'] = pd.to_numeric(self.transactions_df['amount'], errors='coerce')
        logging.info(self.transactions_df[self.transactions_df['amount_numeric'].isna()][['trans_id', 'amount']])

        # Check for potential data quality issues in the 'trans_date' column
        logging.info("\nPotential data quality issues in the 'trans_date' column:")
        self.transactions_df['trans_date_as_date'] = pd.to_datetime(self.transactions_df['trans_date'], errors='coerce')
        logging.info(self.transactions_df[self.transactions_df['trans_date_as_date'].isna()][['trans_id', 'trans_date']])
        
    def export_to_csv(self, output_dir='output'):
        output_dir = os.path.join(self.dir, output_dir)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        users_file = os.path.join(output_dir, 'users.csv')
        transactions_file = os.path.join(output_dir, 'transactions.csv')
        
        self.users_df.to_csv(users_file, index=False)
        self.transactions_df.to_csv(transactions_file, index=False)
        
        # print(f"Users data exported to {users_file}")
        # print(f"Transactions data exported to {transactions_file}")
        
    def handle_trans_date(self):
        # Find the date in amount column
        self.transactions_df['amount_as_date'] = pd.to_datetime(self.transactions_df['amount'], errors='coerce')
        
        # Attempt to parse 'trans_date' with different formats
        self.transactions_df['trans_date_as_date'] = pd.to_datetime(self.transactions_df['trans_date'], errors='coerce')
        for fmt in ('%Y/%m/%d', '%Y-%m-%d'):
            mask = self.transactions_df['trans_date_as_date'].isna()
            self.transactions_df.loc[mask, 'trans_date'] = pd.to_datetime(self.transactions_df.loc[mask, 'trans_date'], format=fmt, errors='coerce')
        
        # Finalize datatypes:
        self.transactions_df['trans_date'] = pd.to_datetime(self.transactions_df['trans_date'], errors='coerce')
        self.transactions_df['trans_date_as_date'] = pd.to_datetime(self.transactions_df['trans_date'], errors='coerce')
        
        # Identify rows where 'amount' is a valid date and 'trans_date' is not a valid date
        valid_date_mask = (
            ~self.transactions_df['amount_as_date'].isna() & 
            self.transactions_df['trans_date_as_date'].isna()
        )
        date_in_amount_rows = self.transactions_df[valid_date_mask]

        if not date_in_amount_rows.empty:
            logging.warning(f"Found {len(date_in_amount_rows)} row/s with valid dates in 'amount' and invalid 'trans_date' values.")
            self.transactions_df.loc[valid_date_mask, 'trans_date'] = self.transactions_df.loc[valid_date_mask, 'amount_as_date']
            self.transactions_df.loc[valid_date_mask, 'amount'] = None

            logging.info(f"Transferred dates from 'amount' to 'trans_date' where 'trans_date' was invalid.")
        
    def handle_non_numeric_amounts(self):
        self.transactions_df['amount_numeric'] = pd.to_numeric(self.transactions_df['amount'], errors='coerce')
        non_numeric_rows = self.transactions_df[self.transactions_df['amount_numeric'].isna()]

        if not non_numeric_rows.empty:
            logging.warning(f"Found {len(non_numeric_rows)} row/s with non-numeric 'amount' values.")
            product_price_map = self.pricing_df.set_index('product')['price'].to_dict()

            self.transactions_df.loc[self.transactions_df['amount_numeric'].isna(), 'amount'] = self.transactions_df.loc[self.transactions_df['amount_numeric'].isna(), 'product'].map(product_price_map)
            
            logging.info(f"Corrected non-numeric 'amount' values based on 'product' prices.")
        
        self.transactions_df['amount'] = pd.to_numeric(self.transactions_df['amount'])
        
    def handle_missing_user_ids(self):
        # Identify and log missing user_ids
        missing_user_ids = self.transactions_df[~self.transactions_df['user_id'].isin(self.users_df['user_id'])]
        
        if not missing_user_ids.empty:
            logging.warning(f"Found {len(missing_user_ids)} transactions with missing user_ids.")
            logging.warning(f"Missing user_ids: {missing_user_ids['user_id'].unique()}")
            # Remove transactions with missing user_ids
            self.transactions_df = self.transactions_df[self.transactions_df['user_id'].isin(self.users_df['user_id'])]
        
    def transform_data(self):
        # Convert all email addresses in users.csv to lowercase
        self.users_df['email'] = self.users_df['email'].str.lower()

        # Convert all product names in transactions.csv to uppercase
        self.transactions_df['product'] = self.transactions_df['product'].str.upper()

        # Calculate the total amount spent by each user
        total_spent = self.transactions_df.groupby('user_id')['amount'].sum().reset_index()
        total_spent.columns = ['user_id', 'total_spent']
        self.users_df = pd.merge(self.users_df, total_spent, on='user_id', how='left')
        
        logging.info(f"Transformed data for users with total amount spent by each user.")
        
    def load_data_to_db(self):
        # Drop temporary columns
        if 'amount_numeric' in self.transactions_df.columns:
            self.transactions_df.drop(columns=['amount_numeric', 'trans_date_as_date'], inplace=True)

        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()

            # Create tables if they do not exist
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS Users (
                user_id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT,
                date_joined TEXT,
                total_spent REAL
            )
            ''')
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS Transactions (
                trans_id INTEGER PRIMARY KEY,
                user_id INTEGER,
                product TEXT,
                amount REAL,
                trans_date TEXT,
                FOREIGN KEY (user_id) REFERENCES Users(user_id)
            )
            ''')

            # Load existing transactions from the database
            existing_transactions = pd.read_sql_query('SELECT * FROM Transactions', conn)

            # Identify new transactions by comparing 'trans_id'
            new_transactions = self.transactions_df[~self.transactions_df['trans_id'].isin(existing_transactions['trans_id'])]
            

            if not new_transactions.empty:
                # Append new transactions to the database
                new_transactions.to_sql('Transactions', conn, if_exists='append', index=False)
                logging.info(f"Appended {len(new_transactions)} new transactions to the database.")
                print(f"Appended {len(new_transactions)} new transactions to the database.")
            else:
                logging.info("No new transactions to append.")
                print("No new transactions to append.")

            # Update the Users table with the new total_spent values
            self.users_df.to_sql('Users', conn, if_exists='replace', index=False)

            conn.commit()
            logging.info(f"Loaded data into the database {self.db_file}.")
            
        except Exception as e:
            print(f'Error loading data into the database: {e}')
            logging.error(f"Error loading data into the database: {e}")
            
        finally:
            conn.close()

        
    def clean_data(self):
        # Explore data to identify potential data quality issues in the source files
        self.explore_data()
        
        # Correct the identified rows that have date values in the amount column that are supposed to be in the trans_date column
        self.handle_trans_date()
        
        # Correct non-numeric amounts
        self.handle_non_numeric_amounts()
        
        # Handle missing user_ids
        self.handle_missing_user_ids()
        
        # Drop temporary columns
        self.transactions_df.drop(columns=['amount_numeric', 'amount_as_date', 'trans_date_as_date'], inplace=True)
        
        # Explore again the data after handling potential data quality issues
        # now the no more data type issue in amount and trans_date column
        self.explore_data()
        
        # Transform data according to requirements
        self.transform_data()
        
        
        
    def save_corrected_transactions(self, output_file='corrected_transactions.csv'):
        output_path = os.path.join(self.dir, output_file)
        self.transactions_df.to_csv(output_path, index=False)
        logging.info(f"Corrected transactions saved to {output_path}")

def main():
    raw_file_dir = os.path.dirname(os.path.abspath(__file__))
    etl_processor = ETLProcessor(raw_file_dir)
    
    # Clean data by correcting potential data quality issues in the 'amount' and 'trans_date' columns
    etl_processor.clean_data()

    # Load the data into a SQLite
    etl_processor.load_data_to_db()
    
    # etl_processor.export_to_csv()
    
if __name__ == '__main__':
    main()