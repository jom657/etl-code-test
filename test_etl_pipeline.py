import pytest
import pandas as pd
from etl_pipeline import ETLProcessor
import sqlite3
import os
import shutil

@pytest.fixture(scope="module")
def setup_dir():
    test_dir = os.path.dirname(os.path.abspath(__file__))

    yield test_dir
    
def test_load_data(setup_dir):
    processor = ETLProcessor(setup_dir)
    processor.load_data()
    assert not processor.transactions_df.empty, "Transactions data should be loaded."
    assert not processor.users_df.empty, "Users data should be loaded."

def test_handle_non_numeric_amounts(setup_dir):
    processor = ETLProcessor(setup_dir)
    processor.load_data()
    processor.handle_non_numeric_amounts()
    assert processor.transactions_df['amount'].notna().all(), "All amounts should be numeric after processing."

def test_database_operations(setup_dir):
    processor = ETLProcessor(setup_dir)
    processor.load_data()
    processor.load_data_to_db()
    conn = sqlite3.connect(processor.db_file)
    existing_transactions = pd.read_sql_query('SELECT * FROM Transactions', conn)
    assert not existing_transactions.empty, "Transactions table should not be empty."
    conn.close()
