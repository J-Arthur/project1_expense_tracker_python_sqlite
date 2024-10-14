import sqlite3
import pandas as pd
import os
import datetime
from fuzzywuzzy import fuzz
print("Hello World")
global current_user
global current_user_id


def on_startup():
    if not os.path.exists('test.db'):
        create_db()
        print("Db created")
        create_default_user()
    else:
        print("Db already exists")

def create_db():

    conn = sqlite3.connect('test.db')
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, dob DATE, yearly_income REAL, hourly_rate REAL, weekly_hours INT, net_balance REAL, gross_balance REAL, date_created DATE )''')
    cur.execute('''CREATE TABLE IF NOT EXISTS m_transactions (id INTEGER PRIMARY KEY, date_uploaded DATE, date_transaction DATE, description TEXT, amount REAL, processed_description TEXT, card_num INT, user_id INT, sector INT, category INT, subcategory INT, niche INT, FOREIGN KEY (user_id) REFERENCES users(id) FOREIGN KEY (card_num) REFERENCES cards(id) FOREIGN KEY (sector) REFERENCES sectors(id) FOREIGN KEY (category) REFERENCES categories(id) FOREIGN KEY (subcategory) REFERENCES subcategories(id) FOREIGN KEY (niche) REFERENCES niches(id))''')
    cur.execute('''CREATE TABLE IF NOT EXISTS cards (id INTEGER PRIMARY KEY, card_num INT, user_id INT, FOREIGN KEY (user_id) REFERENCES users(id))''')
    cur.execute('''CREATE TABLE IF NOT EXISTS budget_limits (id INTEGER PRIMARY KEY, user_id INT, sector INT, category INT, subcategory INT, niche INT, amount_limit REAL, FOREIGN KEY (user_id) REFERENCES users(id) FOREIGN KEY (sector) REFERENCES sectors(id) FOREIGN KEY (category) REFERENCES categories(id) FOREIGN KEY (subcategory) REFERENCES subcategories(id) FOREIGN KEY (niche) REFERENCES niches(id))''')
    cur.execute('''CREATE TABLE IF NOT EXISTS budget_history (id INTEGER PRIMARY KEY, user_id INT, date DATE, year INT, month INT, number_transactions INTEGER, total_in REAL, total_out REAL, total_unique_descriptions INTEGER, FOREIGN KEY (user_id) REFERENCES users(id))''')
    cur.execute('''CREATE TABLE IF NOT EXISTS budget_running_summary (id INTEGER PRIMARY KEY, user_id INT, date DATE, year INT, month INT, number_transactions INTEGER, total_in REAL, total_out REAL, total_unique_descriptions INTEGER, FOREIGN KEY (user_id) REFERENCES users(id))''')
    cur.execute('''CREATE TABLE IF NOT EXISTS sectors (id INTEGER PRIMARY KEY, name TEXT, user_description TEXT, is_credit BOOLEAN, date_created DATE, created_by INT, FOREIGN KEY (created_by) REFERENCES users(id))''')
    cur.execute('''CREATE TABLE IF NOT EXISTS categories (id INTEGER PRIMARY KEY, name TEXT, user_description TEXT, sector INT, is_credit BOOLEAN, date_created DATE, created_by INT, FOREIGN KEY (sector) REFERENCES sectors(id) FOREIGN KEY (created_by) REFERENCES users(id))''')
    cur.execute('''CREATE TABLE IF NOT EXISTS subcategories (id INTEGER PRIMARY KEY, name TEXT, user_description TEXT, sector INT, category INT, is_credit BOOLEAN, date_created DATE, created_by INT, FOREIGN KEY (sector) REFERENCES sectors(id) FOREIGN KEY (category) REFERENCES categories(id) FOREIGN KEY (created_by) REFERENCES users(id))''')
    cur.execute('''CREATE TABLE IF NOT EXISTS niches (id INTEGER PRIMARY KEY, name TEXT, user_description TEXT, sector INT, category INT, subcategory INT, is_credit BOOLEAN, date_created DATE, created_by INT,FOREIGN KEY (sector) REFERENCES sectors(id) FOREIGN KEY (category) REFERENCES categories(id) FOREIGN KEY (subcategory) REFERENCES subcategories(id) FOREIGN KEY (created_by) REFERENCES users(id))''')
    conn.commit()
    conn.close()

def create_default_user():
    global current_user
    global current_user_id
    current_user = 'Default User'
    conn = sqlite3.connect('test.db')
    cur = conn.cursor()
    cur.execute("INSERT INTO users (name, dob, yearly_income, hourly_rate, weekly_hours, net_balance, gross_balance, date_created) VALUES ('Default User', '1990-01-01', 80000, 38.46, 40, 0, 0, ?)", (datetime.datetime.now(),))
    current_user_id = cur.lastrowid
    conn.commit()
    conn.close()

def process_transactions():
    global current_user
    global current_user_id
    conn = sqlite3.connect('test.db')
    cur = conn.cursor()
    cur.execute('''SELECT DISTINCT processed_description, category, subcategory, niche, sector FROM m_transactions''')
    unique_pairs = cur.fetchall()
    unique_pairs = pd.DataFrame(unique_pairs, columns=['processed_description', 'category', 'sub_category', 'niche', 'sector']) 
    print("pairs retreived")
    cur.execute('''SELECT card_num FROM cards WHERE user_id = "current_user_id"''')
    card_num = cur.fetchall()
    schema = ['date_transaction', 'amount', 'description', 'balance']
    new_transactions = pd.read_csv(input("Please enter current file name: ") + '.csv',  names = schema, header = None)
    new_transactions = pre_process_transactions(unique_pairs, new_transactions, card_num)
    print(new_transactions)
    #new_transactions = categorise_transaction(new_transactions, unique_pairs)
    #new_transactions = manual_edits(new_transactions)
    #cur.execute('''INSERT INTO m_transactions (date_uploaded, date_transaction, description, amount, processed_description, card_num, user_id, sector, category, subcategory, niche) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''', (datetime.datetime.now(), new_transactions['date_transaction'], new_transactions['description'], new_transactions['amount'], new_transactions['processed_description'], new_transactions['card'], current_user_id, new_transactions['sector'], new_transactions['category'], new_transactions['subcategory'], new_transactions['niche']))
    print("Transactions processed")

def pre_process_transactions(unique_pairs, new_transactions, card_num):
    max_len = unique_pairs['processed_description'].str.len().fillna(50).max()
    if pd.isna(max_len):
        max_len = 50
    new_transactions['processed_description'] = new_transactions['description'].str.lower()
    new_transactions['processed_description'] = new_transactions['processed_description'].str.replace('[^a-zA-Z]', '', regex=True)
    print(new_transactions['processed_description'])
    exit()
    new_transactions['processed_description'] = new_transactions['processed_description'].str.ljust(int(max_len), fillchar=' ')
    new_transactions['amount'] = new_transactions['amount'].replace({'-': '', '+': ''}, regex=True)
    print("Descriptions processed")
    patterns = card_num
    new_transactions['card'] = None
    for pattern in patterns:
            matches = new_transactions['description'].str.extract(pattern)
            new_transactions['card'] = new_transactions['card'].combine_first(matches[0])
            print("Card numbers extracted")
    
    return new_transactions



on_startup()
process_transactions()
