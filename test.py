import sqlite3
import pandas as pd
import os
import datetime
print("Hello World")


def on_ready():
    if not os.path.exists('users.db'):
        conn = sqlite3.connect('users.db')
        cur = conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, name TEXT NOT NULL, dob DATE, yearly_income REAL, hourly_rate REAL, weekly_hours INT, net_balance REAL, gross_balance REAL, date_created DATE )''')
        cur.execute('''CREATE TABLE IF NOT EXISTS m_transactions (id INT PRIMARY KEY, date_uploaded DATE, date_transaction DATE, description TEXT, amount REAL, processed_description TEXT, card_num INT, user_id INT, sector INT, category INT, subcategory INT, niche INT, FOREIGN KEY (user_id) REFERENCES users(id) FOREIGN KEY (card_num) REFERENCES cards(id) FOREIGN KEY (sector) REFERENCES sectors(id) FOREIGN KEY (category) REFERENCES categories(id) FOREIGN KEY (subcategory) REFERENCES subcategories(id) FOREIGN KEY (niche) REFERENCES niches(id))''')
        cur.execute('''CREATE TABLE IF NOT EXISTS cards (id INT PRIMARY KEY, card_num INT, user_id INT, FOREIGN KEY (user_id) REFERENCES users(id))''')
        cur.execute('''CREATE TABLE IF NOT EXISTS budget_limits (id INT PRIMARY KEY, user_id INT, sector INT, category INT, subcategory INT, niche INT, limit REAL, FOREIGN KEY (user_id) REFERENCES users(id) FOREIGN KEY (sector) REFERENCES sectors(id) FOREIGN KEY (category) REFERENCES categories(id) FOREIGN KEY (subcategory) REFERENCES subcategories(id) FOREIGN KEY (niche) REFERENCES niches(id))''')
        cur.execute('''CREATE TABLE IF NOT EXISTS budget_history (id INT PRIMARY KEY, user_id INT, date DATE, number_transactions INTEGER, total_in REAL, total_out REAL, total_unique_descriptions INTEGER, FOREIGN KEY (user_id) REFERENCES users(id))''')
        cur.execute('''CREATE TABLE IF NOT EXISTS budget_running_summary (id INT PRIMARY KEY, user_id INT, date DATE, number_transactions INTEGER, total_in REAL, total_out REAL, total_unique_descriptions INTEGER, FOREIGN KEY (user_id) REFERENCES users(id))''')
        cur.execute('''CREATE TABLE IF NOT EXISTS sectors (id INT PRIMARY KEY, name TEXT, user_description TEXT, is_credit BOOLEAN, date_created DATE, created_by INT, FOREIGN KEY (created_by) REFERENCES users(id))''')
        cur.execute('''CREATE TABLE IF NOT EXISTS categories (id INT PRIMARY KEY, name TEXT, user_description TEXT, sector INT, is_credit BOOLEAN, date_created DATE, created_by INT, FOREIGN KEY (sector) REFERENCES sectors(id) FOREIGN KEY (created_by) REFERENCES users(id))''')
        cur.execute('''CREATE TABLE IF NOT EXISTS subcategories (id INT PRIMARY KEY, name TEXT, user_description TEXT, sector INT, category INT, is_credit BOOLEAN, date_created DATE, created_by INT, FOREIGN KEY (sector) REFERENCES sectors(id) FOREIGN KEY (category) REFERENCES categories(id) FOREIGN KEY (created_by) REFERENCES users(id))''')
        cur.execute('''CREATE TABLE IF NOT EXISTS niches (id INT PRIMARY KEY, name TEXT, user_description TEXT, sector INT, category INT, subcategory INT, is_credit BOOLEAN, date_created DATE, created_by INT,FOREIGN KEY (sector) REFERENCES sectors(id) FOREIGN KEY (category) REFERENCES categories(id) FOREIGN KEY (subcategory) REFERENCES subcategories(id) FOREIGN KEY (created_by) REFERENCES users(id))''')

        
        #cur.execute('''CREATE TABLE IF NOT EXISTS budget (ID, year INTEGER, month INTEGER, number_transactions INTEGER, total_in REAL, total_out REAL, total_unique_descriptions INTEGER)''')
        conn.commit()
        conn.close()
        ##create_new_user()
        print("Db created")
        create_new_user()
    else:
        print("Db already exists")
        main_menu()

def create_new_user():
     name = input("Please enter your name: ")
     print("Hello " + name)
     if input("Do you know your expected annual income? (y/n) ") == 'y':
         year_income = float(input("Please enter your expected annual income: "))
         hourly_rate = year_income / 52 / 40
         estimated_weekly_hours = year_income / 52
         conn = sqlite3.connect('users.db')
         cur = conn.cursor()
         cur.execute('''INSERT INTO users (name, year_income, hourly_rate, estimated_weekly_hours) VALUES (?,?,?,?)''', (name, year_income, hourly_rate, estimated_weekly_hours))
         conn.commit()
         conn.close()
         create_new_budget(name)
         #print("User " + name + " created with annual income of " + str(year_income))
     else:
            hourly_rate = float(input("Please enter your hourly rate: "))
            estimated_weekly_hours = float(input("Please enter your estimated weekly hours: "))
            year_income = hourly_rate * estimated_weekly_hours * 52
            conn = sqlite3.connect('users.db')
            cur = conn.cursor()
            cur.execute('''INSERT INTO users (name, year_income, hourly_rate, estimated_weekly_hours) VALUES (?,?,?,?)''', (name, year_income, hourly_rate, estimated_weekly_hours))
            conn.commit()
            conn.close()
            #print("User " + name + " created with annual income of " + str(year_income))
            create_new_budget(name)

def create_new_budget(name):
    conn = sqlite3.connect('users.db')
    cur = conn.cursor()
    cur.execute('''SELECT name FROM users WHERE ID = (SELECT MAX(ID) FROM users)''')
    name = cur.fetchone()
    print(f"You are starting a new budget on {datetime.datetime.now()} as {name}")
    #update_frequency = input("Please enter how often you would like to update your budget (daily, weekly, monthly): ")
    cur.execute('''INSERT INTO budget_running_summary (year, month, number_transactions, total_in, total_out, total_unique_descriptions) VALUES (?,?,?,?,?,?)''', (datetime.datetime.now().year, datetime.datetime.now().month, 0, 0, 0, 0))
    conn.commit()
    conn.close()
    if input("Would you like to set some savings goals? (y/n) ") == 'y':
        print("Lets enter some savings goals")
        #set_savings_goals()
    else:
        print("We are going to get you to process some transactions to get started. You will need to categorise these manually to begin with but the longer you use this the fewer transactions you will need to manually adjust.")
        #process_transactions()

def process_transactions():
    conn = sqlite3.connect('users.db')
    cur = conn.cursor()
    cur.execute('''SELECT description, category, sub_category, niche, sector FROM m_transactions''')
    unique_pairs = cur.fetchall()
    unique_pairs = pd.DataFrame(unique_pairs, columns=['description', 'category', 'sub_category', 'niche', 'sector']) 
    print("pairs retreived")
    cur.execute('''SELECT cnum FROM cards WHERE u_id = "Current User"''')
    cnum = cur.fetchall()
    new_transactions = pd.read_csv(input("Please enter current file name: ") + '.csv')
    pre_process_transactions(unique_pairs, new_transactions, cnum)

def pre_process_transactions(unique_pairs, new_transactions, cnum):
    new_transactions['processed_description'] = new_transactions['description'].str.lower()
    new_transactions['processed_description'] = new_transactions['processed_description'].str.replace('[^a-zA-Z]', '')
    new_transactions['processed_description'] = new_transactions['processed_description'].str.ljust(unique_pairs['description'].str.len().max(), fillchar=' ')
    patterns = cnum
    new_transactions['cardnum'] = None
    for pattern in patterns:
        matches = new_transactions['description'].str.extract(pattern)
        new_transactions['card'] = new_transactions['card'].combine_first(matches[0])
        print("Card numbers extracted")       
def main_menu():
    print("Main Menu")
    print("1. Process Transactions")
    print("2. Set Savings Goals")
    print("3. View Budget")
    print("4. View Transactions")
    print("5. View Summary")
    print("6. Exit")
    
    if input("Please select an option: ") == '1':
        process_transactions()        
        
on_ready()