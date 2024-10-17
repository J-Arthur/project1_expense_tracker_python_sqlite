import sqlite3
import pandas as pd
import os
from datetime import datetime
from fuzzywuzzy import fuzz
import re
import calendar
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
    cur.execute('''CREATE TABLE IF NOT EXISTS categories (id INTEGER PRIMARY KEY, name TEXT, user_description TEXT, sector INT, is_credit BOOLEAN, is_essential BOOLEAN date_created DATE, created_by INT, FOREIGN KEY (sector) REFERENCES sectors(id) FOREIGN KEY (is_credit) REFERENCES sectors(is_credit) FOREIGN KEY (created_by) REFERENCES users(id))''')
    cur.execute('''CREATE TABLE IF NOT EXISTS subcategories (id INTEGER PRIMARY KEY, name TEXT, user_description TEXT, sector INT, category INT, is_credit BOOLEAN, date_created DATE, created_by INT, FOREIGN KEY (sector) REFERENCES sectors(id) FOREIGN KEY (category) REFERENCES categories(id) FOREIGN KEY (is_credit) REFERENCES sectors(is_credit) FOREIGN KEY (created_by) REFERENCES users(id))''')
    cur.execute('''CREATE TABLE IF NOT EXISTS niches (id INTEGER PRIMARY KEY, name TEXT, user_description TEXT, sector INT, category INT, subcategory INT, is_credit BOOLEAN, date_created DATE, created_by INT,FOREIGN KEY (sector) REFERENCES sectors(id) FOREIGN KEY (category) REFERENCES categories(id) FOREIGN KEY (subcategory) REFERENCES subcategories(id) FOREIGN KEY (is_credit) REFERENCES sectors(is_credit) FOREIGN KEY (created_by) REFERENCES users(id))''')
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
    def get_files():
        conn = sqlite3.connect('test.db')
        cur = conn.cursor()
        cur.execute('''SELECT DISTINCT processed_description, category, subcategory, niche, sector FROM m_transactions''')
        unique_pairs = cur.fetchall()
        unique_pairs = pd.DataFrame(unique_pairs, columns=['processed_description', 'category', 'subcategory', 'niche', 'sector']) 
        nums = cur.execute('''SELECT card_num FROM cards WHERE user_id = "current_user_id"''')
        card_num = nums.fetchall()
        schema = ['date_transaction', 'amount', 'description', 'balance']
        new_transactions = pd.read_csv(input("Please enter current file name: ") + '.csv',  names = schema, header = None)
        new_transactions['processed_description'] = ''
        new_transactions['card'] = None
        new_transactions['category'] = ''
        new_transactions['subcategory'] = ''
        new_transactions['niche'] = ''
        new_transactions['sector'] = ''
        return unique_pairs, new_transactions, card_num
    unique_pairs, new_transactions, card_num = get_files()
    new_transactions = pre_process_transactions(unique_pairs, new_transactions, card_num)
    new_transactions = categorise_transactions(new_transactions, unique_pairs)
    #new_transactions = manual_edits(new_transactions)
    new_transactions['date_transaction'] = pd.to_datetime(new_transactions['date_transaction'], format='%d/%m/%Y')
    for index, row in new_transactions.iterrows():
        date_transaction_dt = row['date_transaction'].to_pydatetime()
        conn = sqlite3.connect('test.db')
        cur = conn.cursor()
        cur.execute('''INSERT INTO m_transactions (date_uploaded, date_transaction, description, amount, processed_description, card_num, user_id, sector, category, subcategory, niche) VALUES (?,?,?,?,?,?,?,?,?,?,?)''', 
                (datetime.now(), date_transaction_dt, row['description'], row['amount'], row['processed_description'], row['card'], current_user_id, row['sector'], row['category'], row['subcategory'], row['niche']))
        conn.commit()
        conn.close()    
    filename = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_processed_{current_user}.csv"
    new_transactions.to_csv(filename, index=False)
    print("Transactions processed")

def pre_process_transactions(unique_pairs, new_transactions, card_num):
   ###Define 
    def update_transaction_date(new_transactions):
        
        def extract_date(description):
            potential_date = description[-10:]
            try:
                new_date = datetime.strptime(potential_date, '%d/%m/%Y')
                #new_date = new_date.to_pydatetime()
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
            new_transactions.loc[index, ['category', 'subcategory', 'niche', 'sector']] = potential_matches[0][['category', 'subcategory', 'niche', 'sector']]
        elif len(potential_matches) > 1:
            print(f"Potential matches for {row['description']} with amount {row['amount']} are:")
            for i, match in enumerate(potential_matches):
                print(f"{i+1}. {match['description']}, {match['category']}, {match['subcategory']}, {match['niche']}, {match['sector']}")
            
            add_new = len(potential_matches) + 1
            print(f"If none of the above are correct please enter {add_new}")
            selection = int(input("Please select the correct match: "))
            if selection == add_new:
                sector_id, category_id, subcategory_id, niche_id = assign_category(row)
                new_transactions.loc[index, ['category', 'subcategory', 'niche', 'sector']] = [category_id, subcategory_id, niche_id, sector_id]
                common_columns = new_transactions.columns.intersection(unique_pairs.columns)
                row_to_move = new_transactions.loc[index, common_columns]
                unique_pairs = pd.concat([unique_pairs, row_to_move.to_frame().T], ignore_index=True)            
            else:
                new_transactions.loc[index, ['category', 'subcategory', 'niche', 'sector']] = potential_matches[selection-1][['category', 'subcategory', 'niche', 'sector']]
                print("Transactions categorised")
        else:
            print(f"No matches found for {row['description']} with amount {row['amount']}")
            sector_id, category_id, subcategory_id, niche_id = assign_category(row)
            new_transactions.loc[index, ['category', 'subcategory', 'niche', 'sector']] = [category_id, subcategory_id, niche_id, sector_id]
            common_columns = new_transactions.columns.intersection(unique_pairs.columns)
            row_to_move = new_transactions.loc[index, common_columns]
            unique_pairs = pd.concat([unique_pairs, row_to_move.to_frame().T], ignore_index=True)               
    return new_transactions

def assign_category(row):
    global current_user
    global current_user_id
    conn = sqlite3.connect('test.db')
    cur = conn.cursor()
    sectors = [{'rowid': row[0], 'name': row[1]} for row in cur.execute('''SELECT rowid, name FROM sectors''').fetchall()]
    categories = [{'rowid': row[0], 'name': row[1]} for row in cur.execute('''SELECT rowid, name FROM categories''').fetchall()]
    subcategories = [{'rowid': row[0], 'name': row[1]} for row in cur.execute('''SELECT rowid, name FROM subcategories''').fetchall()]
    niches = [{'rowid': row[0], 'name': row[1]} for row in cur.execute('''SELECT rowid, name FROM niches''').fetchall()]

    print(f"Adding new category for {row['description']} {row['amount']}")

    sector, new_sector = new_addition_prompt(sectors, "Please select a sector or add a new one:")
    sector_id = cur.execute('''SELECT rowid FROM sectors WHERE name = ?''', (sector,)).fetchone()
    sector_id = sector_id[0] if sector_id else None
    if new_sector:
        is_credit = input("Is this sector a credit sector? (yes/no): ").lower() == 'yes'
        user_description = input("Please enter a description for this new sector: ")
        cur.execute('''INSERT INTO sectors (name, is_credit, created_by, date_created, user_description) VALUES (?,?,?,?,?)''', (sector, is_credit, current_user_id, datetime.now(), user_description))
        conn.commit()
        sector_id = cur.lastrowid

    category, new_category = new_addition_prompt(categories, "Please select a category or add a new one:")
    category_id = cur.execute('''SELECT rowid FROM categories WHERE name = ?''', (category,)).fetchone()
    category_id = category_id[0] if category_id else None
    if new_category:
        is_essential = input("Is this category essential? (yes/no): ").lower() == 'yes'
        user_description = input("Please enter a description for this new category: ")
        cur.execute('''INSERT INTO categories (name, is_essential, created_by, date_created, user_description, sector) VALUES (?,?,?,?,?,?)''', (category, is_essential, current_user_id, datetime.now(), user_description, sector_id))
        conn.commit()
        category_id = cur.lastrowid

    subcategory, new_subcategory = new_addition_prompt(subcategories, "Please select a subcategory or add a new one:")
    subcategory_id = cur.execute('''SELECT rowid FROM subcategories WHERE name = ?''', (subcategory,)).fetchone()
    subcategory_id = subcategory_id[0] if subcategory_id else None
    if new_subcategory:
        user_description = input("Please enter a description for this new subcategory: ")
        cur.execute('''INSERT INTO subcategories (name, created_by, date_created, user_description, sector, category) VALUES (?,?,?,?,?,?)''', (subcategory, current_user_id, datetime.now(), user_description, sector_id, category_id))
        conn.commit()
        subcategory_id = cur.lastrowid

    niche, new_niche = new_addition_prompt(niches, "Please select a niche or add a new one:")
    niche_id = cur.execute('''SELECT rowid FROM niches WHERE name = ?''', (niche,)).fetchone()
    niche_id = niche_id[0] if niche_id else None
    if new_niche:
        user_description = input("Please enter a description for this new niche: ")
        cur.execute('''INSERT INTO niches (name, created_by, date_created, user_description, sector, category, subcategory) VALUES (?,?,?,?,?,?,?)''', (niche, current_user_id, datetime.now(), user_description, sector_id, category_id, subcategory_id))
        conn.commit()
        niche_id = cur.lastrowid
    conn.close()
 
    return sector_id, category_id, subcategory_id, niche_id

def new_addition_prompt(options, prompt_text):
    print(prompt_text)
    for i, option in enumerate(options):
        print(f"{i+1}. {option['rowid']}. {option['name']}")
    print(f"{len(options)+1}. Add new:")
    print(f"{len(options)+2}. Skip.")
    choice = int(input("Please select an option: "))
    if choice == len(options) + 1:
        new_option = None
        return new_option, True
    elif choice == len(options) + 2:
        return None, False
    else:
        chosen_option = options[choice - 1]['name']
        return chosen_option, False
    
def get_days_in_current_month():
    # Get the current year and month
    now = datetime.now()
    current_year = now.year
    current_month = now.month

    # Get the number of days in the current month
    days_in_month = calendar.monthrange(current_year, current_month)[1]

    return days_in_month

generate_transaction_report:

Overview
total_income = SELECT SUM(m.amount) as total_income
FROM m_transactions m
JOIN sectors s ON m.sector_id = s.id
WHERE s.is_credit = True
AND m.date_transaction BETWEEN date('now', 'start of month') AND date('now', 'start of month', '+1 month', '-1 day');

lst_inc = 
SELECT SUM(m.amount) as lst_inc
FROM m_transactions m
JOIN sectors s ON m.sector_id = s.id
WHERE s.is_credit = True
AND m.date_transaction 
BETWEEN date('now', 'start of month', '-1 month') AND date('now', 'start of month', '-1 day');
yr_avg_inc = SELECT SUM(m.amount) as yr_avg_inc
FROM m_transactions m
JOIN sectors s ON m.sector_id = s.id
WHERE s.is_credit = True
AND m.date_transaction BETWEEN date('now', 'start of month', '-12 months') AND date('now', 'start of month', '-1 day');

vs_last_month = lst_inc - total_income
vs_12_month_average = yr_avg_inc/12 - total_income

total_expenditure = SELECT SUM(m.amount) as total_expenditure
FROM m_transactions m
JOIN sectors s ON m.sector_id = s.id
WHERE s.is_credit = False
AND m.date_transaction BETWEEN date('now', 'start of month') AND date('now', 'start of month', '+1 month', '-1 day');

lst_exp = SELECT SUM(m.amount) as lst_exp
FROM m_transactions m
JOIN sectors s ON m.sector_id = s.id
WHERE s.is_credit = False
AND m.date_transaction BETWEEN date('now', 'start of month', '-1 month') AND date('now', 'start of month', '-1 day');
yr_avg_exp = SELECT SUM(m.amount) as yr_avg_exp
FROM m_transactions m
JOIN sectors s ON m.sector_id = s.id
WHERE s.is_credit = False
AND m.date_transaction BETWEEN date('now', 'start of month', '-12 months') AND date('now', 'start of month', '-1 day');

vs_last_month_exp = lst_exp - total_expenditure
vs_12_month_average_exp = yr_avg_exp/12 - total_expenditure
net_balance = total_income - total_expenditure


essential: 
SELECT SUM(m.amount) as essential
FROM m_transactions m
JOIN categories s ON m.category_id = s.id
WHERE s.is_essential = TRUE
AND m.date_transaction 
BETWEEN date('now', 'start of month') 
AND date('now', 'start of month', '+1 month', '-1 day');

essential_percent = essential / total_expenditure * 100

non_essential = SELECT SUM(m.amount) as non_essential
FROM m_transactions m
JOIN categories s ON m.category_id = s.id
WHERE s.is_essential = FALSE
AND m.date_transaction BETWEEN date('now', 'start of month') AND date('now', 'start of month', '+1 month', '-1 day');

lst_non_essential = SELECT SUM(m.amount) as non_essential
FROM m_transactions m
JOIN categories s ON m.category_id = s.id
WHERE s.is_essential = FALSE
AND m.date_transaction BETWEEN date('now', 'start of month') AND date('now', 'start of month', '+1 month', '-1 day');
non_essential_percent = non_essential / total_expenditure * 100

s
Breakdown
total_transactions = SELECT COUNT(id) as total_transactions
FROM m_transactions
WHERE date_transaction BETWEEN date('now', 'start of month') AND date('now', 'start of month', '+1 month', '-1 day');

largest_transaction = SELECT *
FROM m_transactions m
JOIN sectors s ON m.sector_id = s.id
WHERE s.is_credit = False AND m.category != 'rent'
ORDER BY m.amount DESC
LIMIT 1;

sector_total = 
Select sector_id, SUM(amount) as sector_total
FROM m_transactions
WHERE date_transaction BETWEEN date('now', 'start of month') AND date('now', 'start of month', '+1 month', '-1 day')
GROUP BY sector_id;
lst_sector_total =
SELECT sector_id, SUM(amount) as lst_sector_total
FROM m_transactions
WHERE date_transaction BETWEEN date('now', 'start of month', '-1 month') AND date('now', 'start of month', '-1 day')
GROUP BY sector_totals_id;
sector_vs = lst_sector_totals - sector_totals

category_total =
SELECT category_id, SUM(amount) as category_total
FROM m_transactions
WHERE date_transaction BETWEEN date('now', 'start of month') AND date('now', 'start of month', '+1 month', '-1 day')
GROUP BY category_id;
lst_category_total =
SELECT category_id, SUM(amount) as lst_category_total
FROM m_transactions
WHERE date_transaction BETWEEN date('now', 'start of month') AND date('now', 'start of month', '+1 month', '-1 day')
GROUP BY category_totals_id;

category_vs = lst_category_total - category_total

subcat_total =
SELECT subcategory_id, SUM(amount) as subcat_total
FROM m_transactions
WHERE date_transaction BETWEEN date('now', 'start of month') AND date('now', 'start of month', '+1 month', '-1 day')
GROUP BY subcategory_id;
lst_subcategory_total =
SELECT subcategory_id, SUM(amount) as lst_subcategory_total
FROM m_transactions
WHERE date_transaction BETWEEN date('now', 'start of month') AND date('now', 'start of month', '+1 month', '-1 day')
GROUP BY subcategory_totals_id;

subcat_vs = lst_subcategory_total - subcategory_total

niche_total =
SELECT niche_id, SUM(amount) as niche_total
FROM m_transactions
WHERE date_transaction BETWEEN date('now', 'start of month') AND date('now', 'start of month', '+1 month', '-1 day')
GROUP BY niche_id;
lst_niche_total =
SELECT niche_id, SUM(amount) as lst_niche_total
FROM m_transactions
WHERE date_transaction BETWEEN date('now', 'start of month') AND date('now', 'start of month', '+1 month', '-1 day')
GROUP BY niche_id;

niche_vs = lst_niche_totals - niche_totals


#Goal Progress
days = get_days_in_current_month
total_weekly_progress = ( total_expenditure/ days * 7) - (lst_exp/ days * 7)
avg_weekly_progress = (non_essential / days * 7) - (lst_non_essential / days * 7)
avg_wk_sector = (sector_total / days * 7) - (lst_sector_total / days * 7)
avg_wk_cat = (category_total / days * 7) - (lst_category_total / days * 7)
avg_wk_subcat = (subcat_total / days * 7) - (lst_subcat_total / days * 7)
avg_wk_niche = (niche_total / days * 7) - (lst_niche_total / days * 7)











on_startup()
process_transactions()
