import sqlite3
import pandas as pd
import os
import datetime
from fuzzywuzzy import fuzz
print("Hello World")
global current_user
global current_user_id

def on_startup():
    if not os.path.exists('users.db'):
        create_db()
        print("Db created")
        create_new_user()
    else:
        print("Db already exists")
        get_user()


def create_db():
    conn = sqlite3.connect('users.db')
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

def get_user():
    global current_user
    global current_user_id
    conn = sqlite3.connect('users.db')
    cur = conn.cursor()
    cur.execute('''SELECT id, name FROM users ''')
    users = cur.fetchall()
    if not users:
        print("No users found")
        create_new_user()
    else:
        print("Users found")
        for user in users:
            print(f"{user[0]}. {user[1]}")
        current_user_id = input("Please select a user (ID): ")
        current_user = cur.execute('''SELECT name FROM users WHERE id = ?''', (current_user_id,)).fetchone()[0]
        print("Welcome back " + current_user)
    conn.close()

def create_new_user():
     global current_user
     global current_user_id
     current_user = input("Please enter your name: ")
     print("Hello " + current_user)
     dob = input("Please enter your date of birth (yyyy-mm-dd): ")
     if input("Do you know your expected annual income? (y/n) ") == 'y':
         yearly_income = float(input("Please enter your expected annual income: "))
         hourly_rate = yearly_income / 52 / 40
         weekly_hours = 40
         conn = sqlite3.connect('users.db')
         cur = conn.cursor()
         cur.execute('''INSERT INTO users (name, dob, yearly_income, hourly_rate, weekly_hours, net_balance, gross_balance, date_created) VALUES (?,?,?,?,?,?,?,?)''', (current_user, dob, yearly_income, hourly_rate, weekly_hours, 0, 0, datetime.datetime.now()))
         conn.commit()
         current_user_id = cur.lastrowid
         conn.close()
         print("User " + current_user + " created with annual income of " + str(yearly_income))
     else:
            hourly_rate = float(input("Please enter your hourly rate: "))
            weekly_hours = float(input("Please enter your estimated weekly hours: "))
            yearly_income = hourly_rate * weekly_hours * 52
            conn = sqlite3.connect('users.db')
            cur = conn.cursor()
            cur.execute('''INSERT INTO users (name, dob, yearly_income, hourly_rate, weekly_hours, net_balance, gross_balance, date_created) VALUES (?,?,?,?,?,?,?,?)''', (current_user, dob, yearly_income, hourly_rate, weekly_hours, 0, 0, datetime.datetime.now()))
            conn.commit()
            current_user_id = cur.lastrowid
            conn.close()
            print("User " + current_user + " created with calculated annual income of " + str(yearly_income))

def create_new_budget():
    global current_user
    global current_user_id
    conn = sqlite3.connect('users.db')
    cur = conn.cursor()
    print(f"You are starting a new budget on {datetime.datetime.now()} as {current_user}")
    archive_budget(current_user_id)
    cur.execute('''INSERT INTO budget_running_summary (user_id, date, year, month, number_transactions, total_in, total_out, total_unique_descriptions) VALUES (?,?,?,?,?,?,?,?)''', (current_user_id, datetime.datetime.now(), datetime.datetime.now().year, datetime.datetime.now().month, 0, 0, 0, 0))
    conn.commit()
    conn.close()
    if input("Would you like to set some savings goals? (y/n) ") == 'y':
        print("Lets enter some savings goals")
        main_menu()
        #set_savings_goals()
    else:
        print("We are going to get you to process some transactions to get started. You will need to categorise these manually to begin with but the longer you use this the fewer transactions you will need to manually adjust.")
        main_menu()
        #process_transactions()

def archive_budget(current_user_id):
    conn = sqlite3.connect('users.db')
    cur = conn.cursor()
    cur.execute('''SELECT * FROM budget_running_summary WHERE user_id = ? ORDER BY rowid DESC LIMIT 1''', (current_user_id,))
    last_row = cur.fetchone()
    if last_row:
        cur.execute('''INSERT INTO budget_history (user_id, date, year, month, number_transactions, total_in, total_out, total_unique_descriptions)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', last_row[1:])
        cur.execute('''DELETE FROM budget_running_summary WHERE rowid = ?''', (last_row[0],))
        conn.commit()
        conn.close()
        print("Archived last budget summary")
    else:
        print("No budget summary found")
        conn.close()

def process_transactions():
    global current_user
    global current_user_id
    conn = sqlite3.connect('users.db')
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

def get_table_schema(table_name):
    conn = sqlite3.connect('users.db')
    cur = conn.cursor()
    cur.execute(f'PRAGMA table_info({table_name})')
    schema = cur.fetchall()
    conn.close()
    return [column[1] for column in schema]  # Extract column names

def pre_process_transactions(unique_pairs, new_transactions, card_num):
    max_len = unique_pairs['processed_description'].str.len().fillna(50).max()
    if pd.isna(max_len):
        max_len = 50
    new_transactions['processed_description'] = new_transactions['description'].str.lower()
    new_transactions['processed_description'] = new_transactions['processed_description'].str.replace('[^a-zA-Z]', '')
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

def categorise_transaction(new_transactions, unique_pairs):
    global current_user
    global current_user_id

    for index, row in new_transactions.iterrows():
        potential_matches = []

        for _, unique_row in unique_pairs.iterrows():
            ratio = fuzz.ratio(row['processed_description'], unique_row['processed_description'])
            if ratio > 95:
                potential_matches.append(unique_row)

        if len(potential_matches) == 1:
            new_transactions.loc[index, ['category', 'sub_category', 'niche', 'sector']] = potential_matches[0][['category', 'sub_category', 'niche', 'sector']]
        elif len(potential_matches) == 0:
            print(f"No matches found for {row['description']} with amount {row['amount']}")
            category, subcategory, niche, sector, unique_pairs = add_new_category(row['description'], row['amount'], row['date_transaction'], unique_pairs)

        else:    
            print(f"Potential matches for {row['description']} with amount {row['amount']} are:")
            for i, match in enumerate(potential_matches):
                print(f"{i+1}. {match['description']}, {match['category']}, {match['sub_category']}, {match['niche']}, {match['sector']}")
            add_new = len(potential_matches) + 1
            print(f"If none of the above are correct please enter {add_new}")
            selection = int(input("Please select the correct match: "))
            if selection == add_new:
                category, subcategory, niche, sector, unique_pairs = add_new_category(row['description'], row['amount'], row['date_transaction'], unique_pairs)
                new_transactions.loc[index, ['category', 'sub_category', 'niche', 'sector']] = [category, subcategory, niche, sector]
                return new_transactions
            else:
                    new_transactions.loc[index, ['category', 'sub_category', 'niche', 'sector']] = potential_matches[selection-1][['category', 'sub_category', 'niche', 'sector']]
            print("Transactions categorised")
            return new_transactions 

def add_new_category(description, amount, date, unique_pairs):
    global current_user
    global current_user_id
    conn = sqlite3.connect('users.db')
    cur = conn.cursor()
    sectors = [row[0] for row in cur.execute('''SELECT title FROM sectors''').fetchall()]
    categories = [row[0] for row in cur.execute('''SELECT title FROM categories''').fetchall()]
    subcategories = [row[0] for row in cur.execute('''SELECT title FROM sub_categories''').fetchall()]
    niches = [row[0] for row in cur.execute('''SELECT title FROM niche''').fetchall()]
    print(f"Adding new category for {description} {amount} {date}")
    
    sector, new_sector = new_addition_prompt(sectors, "Please select a sector or add a new one:")
    if new_sector:
        is_credit = input("Is this sector a credit sector? (yes/no): ").lower() == 'yes'
        user_description = input("Please enter a description for this new sector: ")
        sectors.append(sector)
        cur.execute('''INSERT INTO sectors (title, is_credit, created_by, date_created, user_description) VALUES (?,?,?,?,?)''', (sector, is_credit, current_user_id, datetime.datetime.now(), user_description))

    category, new_category = new_addition_prompt(categories, "Please select a category or add a new one:")
    if new_category:
        is_essential = input("Is this category essential? (yes/no): ").lower() == 'yes'
        user_description = input("Please enter a description for this new category: ")
        categories.append(category)
        cur.execute('''INSERT INTO categories (title, is_essential, created_by, date_created, user_description) VALUES (?,?,?,?,?)''', (category, is_essential, current_user_id, datetime.datetime.now(), user_description))

    subcategory, new_subcategory = new_addition_prompt(subcategories, "Please select a subcategory or add a new one:")
    if new_subcategory:
        is_essential = input("Is this subcategory essential? (yes/no): ").lower() == 'yes'
        user_description = input("Please enter a description for this new subcategory: ")
        subcategories.append(subcategory)
        cur.execute('''INSERT INTO subcategories (title, is_essential, created_by, date_created, user_description) VALUES (?,?,?,?,?)''', (subcategory, is_essential, current_user_id, datetime.datetime.now(), user_description))

    niche, new_niche = new_addition_prompt(niches, "Please select a niche or add a new one:")
    if new_niche:
        is_essential = input("Is this niche essential? (yes/no): ").lower() == 'yes'
        user_description = input("Please enter a description for this new niche: ")
        niches.append(niche)
        cur.execute('''INSERT INTO niches (title, is_essential, created_by, date_created, user_description) VALUES (?,?,?,?,?)''', (niche, is_essential, current_user_id, datetime.datetime.now(), user_description))
    conn.commit()
    conn.close()
    unique_pairs = unique_pairs.append({'description': description, 'amount': amount, 'date_transaction': date, 'sector': sector, 'category': category, 'subcategory': subcategory, 'niche': niche}, ignore_index=True)
    return category, subcategory, niche, sector, unique_pairs

def new_addition_prompt(options,prompt_text):
    print(prompt_text)
    for i, option in enumerate(options):
        print(f"{i+1}. {option}")
    print(f"{len(options)+1}. Add new:")
    print(f"{len(options)+2}. Skip.")
    choice = int(input("Please select an option: "))
    if choice == len(options) + 1:
        new_option = input("Enter new option: ")
        return new_option, True
    elif choice == len(options) + 2:
        return None, False
    else:
        return options[choice - 1], False

def manual_edits(new_transactions):
    global current_user
    global current_user_id
    conn = sqlite3.connect('users.db')
    cur = conn.cursor()
    print(new_transactions)
    print("Please review the above transactions and make any necessary changes")
    print("Enter the row number to edit or 'done' to finish")
    while True:
        print(new_transactions)
        row = input("Please enter the row number to edit: ")
        if row == 'done':
            break
        else:
           edit_transaction(new_transactions, row)
    return new_transactions
     
def edit_transaction(new_transactions, row):
    change = input("Do you want to edit or add a note to this transaction? (edit/note): ")
    if change == 'note':
        note = input("Please enter a note: ")
        new_transactions.loc[row, 'user_description'] = note
    else:       
        print(new_transactions.loc[row])
        print("1. Amount")
        print("2. Date")
        print("3. Category Info")
        print("4. Card")
        print("5. Go Back")
        edit = input("Please select an option: ")
        if edit == '1':
            new_amount = input("Please enter the new amount: ")
            new_transactions.loc[row, 'amount'] = new_amount
        elif edit == '2':
            new_date = input("Please enter the new date: ")
            new_transactions.loc[row, 'date_transaction'] = new_date
        elif edit == '3':
             category, subcategory, niche, sector = edit_category_info(row, new_transactions.loc[row, 'description'], new_transactions.loc[row, 'amount'], new_transactions.loc[row, 'date_transaction'])
             new_transactions.loc[row, ['category', 'sub_category', 'niche', 'sector']] = [category, subcategory, niche, sector]
        elif edit == '4':
            new_card = input("Please enter the new card number: ")
            new_transactions.loc[row, 'card'] = new_card
        elif edit == '5':
            return new_transactions
        else:
            print("Invalid selection")

def edit_category_info(row, description, amount, date):
    global current_user
    global current_user_id
    conn = sqlite3.connect('users.db')
    cur = conn.cursor()
    sectors = [row[0] for row in cur.execute('''SELECT title FROM sectors''').fetchall()]
    categories = [row[0] for row in cur.execute('''SELECT title FROM categories''').fetchall()]
    subcategories = [row[0] for row in cur.execute('''SELECT title FROM sub_categories''').fetchall()]
    niches = [row[0] for row in cur.execute('''SELECT title FROM niche''').fetchall()]
    print(f"Changing category info for ")
    sector, new_sector = new_addition_prompt(sectors, "Please select a sector or add a new one:")
    if new_sector:
        is_credit = input("Is this sector a credit sector? (yes/no): ").lower() == 'yes'
        user_description = input("Please enter a description for this new sector: ")
        sectors.append(sector)
        cur.execute('''INSERT INTO sectors (title, is_credit, created_by, date_created, user_description) VALUES (?,?,?,?,?)''', (sector, is_credit, current_user_id, datetime.datetime.now(), user_description))

    category, new_category = new_addition_prompt(categories, "Please select a category or add a new one:")
    if new_category:
        is_essential = input("Is this category essential? (yes/no): ").lower() == 'yes'
        user_description = input("Please enter a description for this new category: ")
        categories.append(category)
        cur.execute('''INSERT INTO categories (title, is_essential, created_by, date_created, user_description) VALUES (?,?,?,?,?)''', (category, is_essential, current_user_id, datetime.datetime.now(), user_description))

    subcategory, new_subcategory = new_addition_prompt(subcategories, "Please select a subcategory or add a new one:")
    if new_subcategory:
        is_essential = input("Is this subcategory essential? (yes/no): ").lower() == 'yes'
        user_description = input("Please enter a description for this new subcategory: ")
        subcategories.append(subcategory)
        cur.execute('''INSERT INTO subcategories (title, is_essential, created_by, date_created, user_description) VALUES (?,?,?,?,?)''', (subcategory, is_essential, current_user_id, datetime.datetime.now(), user_description))

    niche, new_niche = new_addition_prompt(niches, "Please select a niche or add a new one:")
    if new_niche:
        is_essential = input("Is this niche essential? (yes/no): ").lower() == 'yes'
        user_description = input("Please enter a description for this new niche: ")
        niches.append(niche)
        cur.execute('''INSERT INTO niches (title, is_essential, created_by, date_created, user_description) VALUES (?,?,?,?,?)''', (niche, is_essential, current_user_id, datetime.datetime.now(), user_description))
    conn.commit()
    conn.close()
    return category, subcategory, niche, sector

def main_menu():
    global current_user
    global current_user_id
    print("Main Menu")
    print("1. Process Transactions")
    print("2. Set Savings Goals")
    print("3. View Budgets")
    print("4. View Transactions")
    print("5. View Reports")
    print("6. Exit")
    choice = input("Please select an option: ")
    if choice == '1':
        process_transactions()
    elif choice == '2':
        pass#set_savings_goals()
    elif choice == '3':
        create_new_budget()
        #view_budgets()
    elif choice == '4':
        pass#view_transactions()
    elif choice == '5':
       pass# view_reports()
    elif choice == '6':
        exit()
    else:
        print("Invalid selection")
        main_menu()       
        
on_startup()
main_menu()