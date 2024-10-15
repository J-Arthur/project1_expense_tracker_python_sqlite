import sqlite3
import pandas as pd
import os
from datetime import datetime
from fuzzywuzzy import fuzz
import re
print("Hello World")
global current_user
global current_user_id

pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', None)     # Show all rows
pd.set_option('display.max_colwidth', None) # Show full column width
pd.set_option('display.width', None)        # Auto-detect the display width

def on_startup():
    if not os.path.exists('test.db'):
        create_db()
        print("Db created")
        create_default_user()
    else:
        global current_user
        global current_user_id 
        current_user = 'Default User'
        current_user_id = 1
        print("Db already exists")
    return current_user, current_user_id

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
    cur.execute('''CREATE TABLE IF NOT EXISTS categories (id INTEGER PRIMARY KEY, name TEXT, user_description TEXT, sector INT, is_credit BOOLEAN, is_essential BOOLEAN, date_created DATE, created_by INT, FOREIGN KEY (sector) REFERENCES sectors(id) FOREIGN KEY (created_by) REFERENCES users(id))''')
    cur.execute('''CREATE TABLE IF NOT EXISTS subcategories (id INTEGER PRIMARY KEY, name TEXT, user_description TEXT, sector INT, category INT, is_credit BOOLEAN, is_essential BOOLEAN, date_created DATE, created_by INT, FOREIGN KEY (sector) REFERENCES sectors(id) FOREIGN KEY (category) REFERENCES categories(id) FOREIGN KEY (created_by) REFERENCES users(id))''')
    cur.execute('''CREATE TABLE IF NOT EXISTS niches (id INTEGER PRIMARY KEY, name TEXT, user_description TEXT, sector INT, category INT, subcategory INT, is_credit BOOLEAN, is_essential BOOLEAN, date_created DATE, created_by INT,FOREIGN KEY (sector) REFERENCES sectors(id) FOREIGN KEY (category) REFERENCES categories(id) FOREIGN KEY (subcategory) REFERENCES subcategories(id) FOREIGN KEY (created_by) REFERENCES users(id))''')
    conn.commit()
    conn.close()

def create_default_user():
    global current_user
    global current_user_id
    current_user = 'Default User'
    conn = sqlite3.connect('test.db')
    cur = conn.cursor()
    cur.execute("INSERT INTO users (name, dob, yearly_income, hourly_rate, weekly_hours, net_balance, gross_balance, date_created) VALUES ('Default User', '1990-01-01', 80000, 38.46, 40, 0, 0, ?)", (datetime.now(),))
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
    #cur.execute('''SELECT card_num FROM cards WHERE user_id = "current_user_id"''')
    card_num = ['4626']
    schema = ['date_transaction', 'amount', 'description', 'balance']
    new_transactions = pd.read_csv(input("Please enter current file name: ") + '.csv',  names = schema, header = None)
    new_transactions = pre_process_transactions(unique_pairs, new_transactions, card_num)
    print(new_transactions)
    new_transactions = categorise_transactions(new_transactions, unique_pairs)
    #new_transactions = manual_edits(new_transactions)
    #cur.execute('''INSERT INTO m_transactions (date_uploaded, date_transaction, description, amount, processed_description, card_num, user_id, sector, category, subcategory, niche) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''', (datetime.datetime.now(), new_transactions['date_transaction'], new_transactions['description'], new_transactions['amount'], new_transactions['processed_description'], new_transactions['card'], current_user_id, new_transactions['sector'], new_transactions['category'], new_transactions['subcategory'], new_transactions['niche']))
    print("Transactions processed")

def pre_process_transactions(unique_pairs, new_transactions, card_num):
   ###Define 
    def update_transaction_date(new_transactions):
        
        def extract_date(description):
            potential_date = description[-10:]
            try:
                new_date = datetime.strptime(potential_date, '%d/%m/%Y')
                return new_date
            except ValueError:
                return None

        # Apply the extract_date function to each row in the DataFrame
        new_transactions['new_date'] = new_transactions['description'].apply(extract_date)
        
        # Update the date_transaction column with the new date where applicable
        new_transactions['date_transaction'] = new_transactions['date_transaction'].where(new_transactions['new_date'].isnull(), new_transactions['new_date'])
        
        # Drop the temporary new_date column
        new_transactions.drop(columns=['new_date'], inplace=True)
        
        return new_transactions
    
    def remove_sequences(processed_description):
        sequences_to_remove = ['xx', 'valuedate', 'aus', 'au', 'card']
        for sequence in sequences_to_remove:
            processed_description = processed_description.replace(sequence, '')
        return processed_description

    def extract_card_number(row, card_num):
        patterns = card_num
        card_number = None
        for pattern in patterns:
            match = re.search(pattern, row['processed_description'])
            if match:
                card_number = match.group(0)
                break
        return card_number  
    
    ### Apply 
    new_transactions['date_transaction'] = pd.to_datetime(new_transactions['date_transaction'], format='%d/%m/%Y')
    new_transactions = update_transaction_date(new_transactions)
    
    max_len = unique_pairs['processed_description'].str.len().fillna(50).max()
    if pd.isna(max_len):
        max_len = 50
    

    new_transactions['processed_description'] = new_transactions['description'].str.lower()
    new_transactions['card'] = new_transactions.apply(lambda row: extract_card_number(row, card_num), axis=1)
    print(new_transactions['card'])
    new_transactions['processed_description'] = new_transactions['processed_description'].str.replace('[^a-zA-Z]', '', regex=True)
    new_transactions['processed_description'] = new_transactions['processed_description'].apply(remove_sequences)
    new_transactions['processed_description'] = new_transactions['processed_description'].str.ljust(int(max_len), fillchar=' ')
    new_transactions['amount'] = abs(new_transactions['amount'])
    print("Descriptions processed")

    return new_transactions

def categorise_transactions(new_transactions, unique_pairs):
    global current_user
    global current_user_id

    for index, row in new_transactions.iterrows():
        potential_matches = []

        for _, unique_row in unique_pairs.iterrows():
            ratio = fuzz.ratio(row['processed_description'], unique_row['processed_description'])
            if ratio > 95:
                potential_matches.append(unique_row)
                
        if len(potential_matches)== 1:
            new_transactions.loc[index, ['category', 'sub_category', 'niche', 'sector']] = potential_matches[0][['category', 'sub_category', 'niche', 'sector']]
        elif len(potential_matches) > 1:
            print(f"Potential matches for {row['description']} with amount {row['amount']} are:")
            for i, match in enumerate(potential_matches):
                print(f"{i+1}. {match['description']}, {match['category']}, {match['sub_category']}, {match['niche']}, {match['sector']}")
            
            add_new = len(potential_matches) + 1
            print(f"If none of the above are correct please enter {add_new}")
            selection = int(input("Please select the correct match: "))
            if selection == add_new:
                modified_row, unique_pairs = assign_category(row, unique_pairs)
                new_transactions.loc[index] = modified_row
                
            else:
                new_transactions.loc[index, ['category', 'sub_category', 'niche', 'sector']] = potential_matches[selection-1][['category', 'sub_category', 'niche', 'sector']]
                print("Transactions categorised")
        else:
            print(f"No matches found for {row['description']} with amount {row['amount']}")
            modified_row, unique_pairs = assign_category(row, unique_pairs)
            new_transactions.loc[index] = modified_row
                
    return new_transactions

def assign_category(row, unique_pairs):
    global current_user
    global current_user_id
    conn = sqlite3.connect('test.db')
    cur = conn.cursor()
    sectors = [{'rowid': row[0], 'name': row[1]} for row in cur.execute('''SELECT rowid, name FROM sectors''').fetchall()]
    categories = [{'rowid': row[0], 'name': row[1]} for row in cur.execute('''SELECT rowid, name FROM categories''').fetchall()]
    subcategories = [{'rowid': row[0], 'name': row[1]} for row in cur.execute('''SELECT rowid, name FROM subcategories''').fetchall()]
    niches = [{'rowid': row[0], 'name': row[1]} for row in cur.execute('''SELECT rowid, name FROM niches''').fetchall()]

    print(f"Adding new category for {row}")

    sector, new_sector = new_addition_prompt(sectors, "Please select a sector or add a new one:")
    sector_id = cur.execute('''SELECT rowid FROM sectors WHERE name = ?''', (sector,)).fetchone()
    if new_sector:
        is_credit = input("Is this sector a credit sector? (yes/no): ").lower() == 'yes'
        user_description = input("Please enter a description for this new sector: ")
        cur.execute('''INSERT INTO sectors (name, is_credit, created_by, date_created, user_description) VALUES (?,?,?,?,?)''', (sector, is_credit, current_user_id, datetime.now(), user_description))
        conn.commit()
        sector_id = cur.lastrowid

    category, new_category = new_addition_prompt(categories, "Please select a category or add a new one:")
    category_id = cur.execute('''SELECT rowid FROM categories WHERE name = ?''', (category,)).fetchone()
    if new_category:
        is_essential = input("Is this category essential? (yes/no): ").lower() == 'yes'
        user_description = input("Please enter a description for this new category: ")
        cur.execute('''INSERT INTO categories (name, is_essential, created_by, date_created, user_description, sector) VALUES (?,?,?,?,?,?)''', (category, is_essential, current_user_id, datetime.now(), user_description, sector_id))
        conn.commit()
        category_id = cur.lastrowid

    subcategory, new_subcategory = new_addition_prompt(subcategories, "Please select a subcategory or add a new one:")
    subcategory_id = cur.execute('''SELECT rowid FROM subcategories WHERE name = ?''', (subcategory,)).fetchone()
    if new_subcategory:
        is_essential = input("Is this subcategory essential? (yes/no): ").lower() == 'yes'
        user_description = input("Please enter a description for this new subcategory: ")
        cur.execute('''INSERT INTO subcategories (name, is_essential, created_by, date_created, user_description, sector, category) VALUES (?,?,?,?,?,?,?)''', (subcategory, is_essential, current_user_id, datetime.now(), user_description, sector_id, category_id))
        conn.commit()
        subcategory_id = cur.lastrowid

    niche, new_niche = new_addition_prompt(niches, "Please select a niche or add a new one:")
    niche_id = cur.execute('''SELECT rowid FROM niches WHERE name = ?''', (niche,)).fetchone()
    if new_niche:
        is_essential = input("Is this niche essential? (yes/no): ").lower() == 'yes'
        user_description = input("Please enter a description for this new niche: ")
        cur.execute('''INSERT INTO niches (name, is_essential, created_by, date_created, user_description, sector, category, subcategory) VALUES (?,?,?,?,?,?,?,?)''', (niche, is_essential, current_user_id, datetime.now(), user_description, sector_id, category_id, subcategory_id))
        conn.commit()
        niche_id = cur.lastrowid
    conn.close()

    #row['category'] = category_id
    #row['subcategory'] = subcategory_id
    #row['niche'] = niche_id
    row.update({'sector': sector_id, 'category': category_id, 'subcategory': subcategory_id, 'niche': niche_id})
    print(row)
    unique_pairs = pd.concat([unique_pairs, row], ignore_index=True)
    print(unique_pairs.head())   
    return row, unique_pairs

def new_addition_prompt(options, prompt_text):
    print(prompt_text)
    for i, option in enumerate(options):
        print(f"{i+1}. {option['rowid']}. {option['name']}")
    print(f"{len(options)+1}. Add new:")
    print(f"{len(options)+2}. Skip.")
    choice = int(input("Please select an option: "))
    if choice == len(options) + 1:
        new_option = input("Enter new option: ")
        return new_option, True
    elif choice == len(options) + 2:
        return None, False
    else:
        chosen_option = options[choice - 1]['name']
        return chosen_option, False
    

on_startup()
process_transactions()
