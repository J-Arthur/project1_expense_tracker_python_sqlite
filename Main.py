# This program should take a csv input file of transactions
# it will categorise them based on previous transactions
# it will then store them in a database and generate a report

#Import libraries
#These are for db creation and manipulation
import sqlite3
import pandas as pd
#These are for filepath and datetime
import os
from datetime import datetime
import calendar
#These are for comparing the transaction descriptions
from fuzzywuzzy import fuzz
import re
#This if for creating the graphs in reports
import matplotlib

#Set display options for pandas
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', None)     # Show all rows
pd.set_option('display.max_colwidth', None) # Show full column width
pd.set_option('display.width', None)        # Auto-detect the display width

#Set the global variables
global current_user
global current_user_id

#Define the functions

#What happens when the program starts
def on_startup():
    if not os.path.exists('users.db'):
#If the db does not exist, create it
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
            cur.execute('''CREATE TABLE IF NOT EXISTS categories (id INTEGER PRIMARY KEY, name TEXT, user_description TEXT, sector INT, is_credit BOOLEAN, is_essential BOOLEAN date_created DATE, created_by INT, FOREIGN KEY (sector) REFERENCES sectors(id) FOREIGN KEY (is_credit) REFERENCES sectors(is_credit) FOREIGN KEY (created_by) REFERENCES users(id))''')
            cur.execute('''CREATE TABLE IF NOT EXISTS subcategories (id INTEGER PRIMARY KEY, name TEXT, user_description TEXT, sector INT, category INT, is_credit BOOLEAN, date_created DATE, created_by INT, FOREIGN KEY (sector) REFERENCES sectors(id) FOREIGN KEY (category) REFERENCES categories(id) FOREIGN KEY (is_credit) REFERENCES sectors(is_credit) FOREIGN KEY (created_by) REFERENCES users(id))''')
            cur.execute('''CREATE TABLE IF NOT EXISTS niches (id INTEGER PRIMARY KEY, name TEXT, user_description TEXT, sector INT, category INT, subcategory INT, is_credit BOOLEAN, date_created DATE, created_by INT,FOREIGN KEY (sector) REFERENCES sectors(id) FOREIGN KEY (category) REFERENCES categories(id) FOREIGN KEY (subcategory) REFERENCES subcategories(id) FOREIGN KEY (is_credit) REFERENCES sectors(is_credit) FOREIGN KEY (created_by) REFERENCES users(id))''')
            conn.commit()
            conn.close()
        create_db()
        #Once db is created, create a new user
        create_new_user()
    else:
        #If db already exists, get the user
        def get_user():
            #pull in the global variables
            global current_user
            global current_user_id
            #connect to the db
            conn = sqlite3.connect('users.db')
            cur = conn.cursor()
            cur.execute('''SELECT id, name FROM users ''')
            users = cur.fetchall()
            #Try to get the user
            #If no users found, create a new user
            if not users:
                print("No users found")
                create_new_user()
            else:
                #If users found, list them
                print("Users found")
                for user in users:
                    print(f"{user[0]}. {user[1]}")
                #Ask for the user for their id
                current_user_id = input("Please select a user (ID): ")
                #Get the user name based on ID provided
                current_user = cur.execute('''SELECT name FROM users WHERE id = ?''', (current_user_id,)).fetchone()[0]
                print("Welcome back " + current_user)
            conn.close()
        get_user()

#How to create a new user
def create_new_user():
    #pull in the global variables
    global current_user
    global current_user_id
    #Get name, dob for new user
    new_user = input("Please enter your name: ")
    dob = input("Please enter your date of birth (yyyy-mm-dd): ")
    #To determine yearly income and hourly rate based on user input
    if input("Do you know your expected annual income? (y/n) ") == 'y':
        yearly_income = float(input("Please enter your expected annual income: "))
        hourly_rate = yearly_income / 52 / 40
        weekly_hours = 40
    else:
        hourly_rate = float(input("Please enter your hourly rate: "))
        weekly_hours = float(input("Please enter your estimated weekly hours: "))
        yearly_income = hourly_rate * weekly_hours * 52
    #connect to db and insert new user
    conn = sqlite3.connect('users.db')
    cur = conn.cursor()
    cur.execute('''INSERT INTO users (name, dob, yearly_income, hourly_rate, weekly_hours, net_balance, gross_balance, date_created) VALUES (?,?,?,?,?,?,?,?)''', (current_user, dob, yearly_income, hourly_rate, weekly_hours, 0, 0, datetime.now()))
    conn.commit()
    #ask if user wants to continue as new or log in as other
    if input("Would you like to continue as " + new_user + " (y/n)? ") == 'y':
        current_user_id = cur.lastrowid
        current_user = new_user
    else:
        on_startup()

#Main function to process transactions
def process_transactions():
    #pull in the global variables
    global current_user
    global current_user_id
    #function to pull in the file
    def get_files():
        #connect to the db
        conn = sqlite3.connect('users.db')
        cur = conn.cursor()
        #get all unique combinations of description and category types
        cur.execute('''SELECT DISTINCT processed_description, category, subcategory, niche, sector FROM m_transactions''')
        unique_pairs = cur.fetchall()
        #create a dataframe of the unique pairs and define the structure
        unique_pairs = pd.DataFrame(unique_pairs, columns=['processed_description', 'category', 'subcategory', 'niche', 'sector']) 
        #get all the card numbers stored for the current user
        nums = cur.execute('''SELECT card_num FROM cards WHERE user_id = "current_user_id"''')
        card_num = nums.fetchall()
        #define the schema for the uploaded file and add columns to match the table in the db
        schema = ['date_transaction', 'amount', 'description', 'balance']
        new_transactions = pd.read_csv(input("Please enter current file name: ") + '.csv',  names = schema, header = None)
        new_transactions['processed_description'] = ''
        new_transactions['card'] = None
        new_transactions['category'] = ''
        new_transactions['subcategory'] = ''
        new_transactions['niche'] = ''
        new_transactions['sector'] = ''
        #return 2 dataframs (unique pairs and new transactions) and a list of card numbers
        return unique_pairs, new_transactions, card_num
    #run the function to get the files and populate the main dataframes
    unique_pairs, new_transactions, card_num = get_files()
    #run the function that cleans the data and extracts date and card number from descriptions
    new_transactions = pre_process_transactions(unique_pairs, new_transactions, card_num)
    #run the function that categorises the transactions based on previous transactions
    new_transactions = categorise_transactions(new_transactions, unique_pairs)
    #new_transactions = manual_edits(new_transactions)
    #convert the date to datetime format for insertion into the db
    new_transactions['date_transaction'] = pd.to_datetime(new_transactions['date_transaction'], format='%d/%m/%Y')
    #Insert each row into the db one at a time
    for index, row in new_transactions.iterrows():
        date_transaction_dt = row['date_transaction'].to_pydatetime()
        conn = sqlite3.connect('users.db')
        cur = conn.cursor()
        cur.execute('''INSERT INTO m_transactions (date_uploaded, date_transaction, description, amount, processed_description, card_num, user_id, sector, category, subcategory, niche) VALUES (?,?,?,?,?,?,?,?,?,?,?)''', 
                (datetime.now(), date_transaction_dt, row['description'], row['amount'], row['processed_description'], row['card'], current_user_id, row['sector'], row['category'], row['subcategory'], row['niche']))
        conn.commit()
        conn.close()
    #save the new transactions to a csv file for records    
    filename = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_processed_{current_user}.csv"
    new_transactions.to_csv(filename, index=False)
    print("Transactions processed")

#function to clean data and extract date and card number from descriptions
def pre_process_transactions(unique_pairs, new_transactions, card_num):
   #get actual transaction date from description
    def update_transaction_date(new_transactions):
        
        def extract_date(description):
            #take the last 10 characters of the description
            potential_date = description[-10:]
            #try to convert the last 10 characters to a date
            try:
                new_date = datetime.strptime(potential_date, '%d/%m/%Y')
                return new_date
            #if it fails, return None
            except ValueError:
                return None

        # Apply the extract_date function to each row in the DataFrame and add a new column to hold returned data
        new_transactions['new_date'] = new_transactions['description'].apply(extract_date)
        
        # Update the date_transaction column with the new_date where new_date is not null. If it is null, keep the original date_transaction
        new_transactions['date_transaction'] = new_transactions['date_transaction'].where(new_transactions['new_date'].isnull(), new_transactions['new_date'])
        # Drop the temporary new_date column
        new_transactions.drop(columns=['new_date'], inplace=True)
        
        return new_transactions
    #function to remove unwanted characters and sequences from the description
    def remove_sequences(processed_description):
        #define the sequences to remove
        sequences_to_remove = ['xx', 'valuedate', 'aus', 'au', 'card']
        #for each instance of processed_description, replace any of the sequences with a space 
        for sequence in sequences_to_remove:
            processed_description = processed_description.replace(sequence, '')
        return processed_description
    #function to extract card number from description
    def extract_card_number(row, card_num):
        #define the pattern to search for as the list of card numbers
        patterns = card_num
        #create a variable to hold the extracted card number
        card_number = None
        #search for each pattern in each processed_description in the dataframe
        for pattern in patterns:
            match = re.search(pattern, row['processed_description'])
            #if there is a match, assign the match to the card_number variable
            if match:
                card_number = match.group(0)
                break
        return card_number  
    
    #convert the date_transaction column to datetime format to allow for uploadin to the db 
    new_transactions['date_transaction'] = pd.to_datetime(new_transactions['date_transaction'], format='%d/%m/%Y')
    #update the dataframe by running the date function
    new_transactions = update_transaction_date(new_transactions)
    #get the max length of the processed_description column for padding later
    max_len = unique_pairs['processed_description'].str.len().fillna(50).max()
    if pd.isna(max_len):
        max_len = 50
    #convert the description to lowercase
    new_transactions['processed_description'] = new_transactions['description'].str.lower()
    #extract the card number from the description
    new_transactions['card'] = new_transactions.apply(lambda row: extract_card_number(row, card_num), axis=1)
    #replace any non-alphabetic characters with a space
    new_transactions['processed_description'] = new_transactions['processed_description'].str.replace('[^a-zA-Z]', '', regex=True)
    #remove unwanted words from the description
    new_transactions['processed_description'] = new_transactions['processed_description'].apply(remove_sequences)
    #pad the processed_description to the max length for comparison later
    new_transactions['processed_description'] = new_transactions['processed_description'].str.ljust(int(max_len), fillchar=' ')
    #convert all numbers to positive
    new_transactions['amount'] = abs(new_transactions['amount'])
    print("Descriptions processed")
    return new_transactions

#main comparison and categorisation function
def categorise_transactions(new_transactions, unique_pairs):
    #pull in the global variables
    global current_user
    global current_user_id
    #of each row in the new_transactions dataframe complete the following process:
    for index, row in new_transactions.iterrows():
        #1 create a list to hold potential matches
        potential_matches = []
        #2 compare to each row of the unique_pairs dataframe one at a time
        for _, unique_row in unique_pairs.iterrows():
            # 3 see how similar processed_description as a ratio. This is why we padded earlier.
            ratio = fuzz.ratio(row['processed_description'], unique_row['processed_description'])
            # 4 if the ratio is greater than 95% add the UNIQUE row to the matches dictionary
            if ratio > 95:
                potential_matches.append(unique_row)
        #5.1 look at the list of matches once all unique_pairs have been compared
        # if there is only 1 match assign category info and move to the next row in new_transactions
        # and repeat the process.        
        if len(potential_matches)== 1:
            new_transactions.loc[index, ['category', 'subcategory', 'niche', 'sector']] = potential_matches[0][['category', 'subcategory', 'niche', 'sector']]
        #5.2 if there are multiple matches, print them out and ask the user to select the correct one
        # or to add a new category
        elif len(potential_matches) > 1:
            print(f"Potential matches for {row['description']} with amount {row['amount']} are:")
            #print out the potential matches
            for i, match in enumerate(potential_matches):
                print(f"{i+1}. {match['description']}, {match['category']}, {match['subcategory']}, {match['niche']}, {match['sector']}")
            #add a new option to the list of potential matches
            add_new = len(potential_matches) + 1
            print(f"If none of the above are correct please enter {add_new}")
            selection = int(input("Please select the correct match: "))
            #if the user selects the new option, call the assign_category function
            #function to assign a category to a new transaction
            def assign_category(new_transactions, unique_pairs, index):
                #pull in the global variables
                global current_user
                global current_user_id
                #connect to the db
                conn = sqlite3.connect('.db')
                cur = conn.cursor()
                #get the list of sectors, categories, subcategories and niches
                sectors = [{'rowid': row[0], 'name': row[1]} for row in cur.execute('''SELECT rowid, name FROM sectors''').fetchall()]
                categories = [{'rowid': row[0], 'name': row[1]} for row in cur.execute('''SELECT rowid, name FROM categories''').fetchall()]
                subcategories = [{'rowid': row[0], 'name': row[1]} for row in cur.execute('''SELECT rowid, name FROM subcategories''').fetchall()]
                niches = [{'rowid': row[0], 'name': row[1]} for row in cur.execute('''SELECT rowid, name FROM niches''').fetchall()]
            
                
            
            
            
            
            
            
            if selection == add_new:
                sector_id, category_id, subcategory_id, niche_id = assign_category(new_transactions, unique_pairs, index)
                new_transactions.loc[index, ['category', 'subcategory', 'niche', 'sector']] = [category_id, subcategory_id, niche_id, sector_id]
                common_columns = new_transactions.columns.intersection(unique_pairs.columns)
                row_to_move = new_transactions.loc[index, common_columns]
                unique_pairs = pd.concat([unique_pairs, row_to_move.to_frame().T], ignore_index=True)            
            else:
                new_transactions.loc[index, ['category', 'subcategory', 'niche', 'sector']] = potential_matches[selection-1][['category', 'subcategory', 'niche', 'sector']]
                print("Transactions categorised")
        #5.3 if there are no matches, alert user and call add_category function
        else:
            print(f"No matches found for {row['description']} with amount {row['amount']}")
            sector_id, category_id, subcategory_id, niche_id = assign_category(new_transactions, unique_pairs, index)
            new_transactions.loc[index, ['category', 'subcategory', 'niche', 'sector']] = [category_id, subcategory_id, niche_id, sector_id]
            common_columns = new_transactions.columns.intersection(unique_pairs.columns)
            row_to_move = new_transactions.loc[index, common_columns]
            unique_pairs = pd.concat([unique_pairs, row_to_move.to_frame().T], ignore_index=True)               
    return new_transactions

#function to create a new category









