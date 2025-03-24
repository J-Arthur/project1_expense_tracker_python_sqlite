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

        #the user is shown all the sectors and asked to select one
        sector = new_addition_prompt(sectors, "Add new", "Please select a sector or add a new one:") 
        #if the user selects one then the ID is assigned to be the thing that is stored in the transaction.
        if sector:
            sector_id = cur.execute('''SELECT rowid FROM sectors WHERE name = ?''', (sector,)).fetchone()
        #if there is no sector the user will trigger the new_sector function
        else:
            type = 'sector'
            sector = new_category(type)
            sector_id = cur.lastrowid
        #repeat for category
        category = new_addition_prompt(categories, "Add new", "Please select a category or add a new one:")
        if category:
            category_id = cur.execute('''SELECT rowid FROM categories WHERE name = ?''', (category,)).fetchone()
        else:
            type = 'category'
            category = new_category(type)
            category_id = cur.lastrowid
        #repeat for subcategory
        subcategory = new_addition_prompt(subcategories, "Add new", "Please select a subcategory or add a new one:")
        if subcategory:
            subcategory_id = cur.execute('''SELECT rowid FROM subcategories WHERE name = ?''', (subcategory,)).fetchone()
        else:
            type = 'subcategory'
            subcategory = new_category(type)
            subcategory_id = cur.lastrowid
        #repeat for niche
        niche = new_addition_prompt(niches, "Add new", "Please select a niche or add a new one:")
        if niche:
            niche_id = cur.execute('''SELECT rowid FROM niches WHERE name = ?''', (niche,)).fetchone()
        else:
            type = 'niche'
            niche = new_category(type)
            niche_id = cur.lastrowid
        new_transactions.loc[index, ['category', 'subcategory', 'niche', 'sector']] = [category_id, subcategory_id, niche_id, sector_id]
        common_columns = new_transactions.columns.intersection(unique_pairs.columns)
        row_to_move = new_transactions.loc[index, common_columns]
        unique_pairs = pd.concat([unique_pairs, row_to_move.to_frame().T], ignore_index=True)            
        return new_transactions, unique_pairs        
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
            if selection == add_new:
                new_transactions, unique_pairs = assign_category(new_transactions, unique_pairs, index)
                
            else:
                new_transactions.loc[index, ['category', 'subcategory', 'niche', 'sector']] = potential_matches[selection-1][['category', 'subcategory', 'niche', 'sector']]
                print("Transactions categorised")
        #5.3 if there are no matches, alert user and call assign_category function
        else:
            print(f"No matches found for {row['description']} with amount {row['amount']}")
            new_transactions, unique_pairs = assign_category(new_transactions, unique_pairs, index)
              
    return new_transactions

#function to create a new category
def new_category(type):
    #pull in the global variables 
    global current_user
    global current_user_id
    name = input(f"Please enter a name for the new {type}: ")
    user_description = input(f"Please enter a description for the new {type}: ")
    if type == 'sector':
        cue = 'Credit'
        column = 'is_credit'
        is_credit_essential = input(f"Is this {type} {cue}? (yes/no): ").lower() == 'yes'
    elif type == 'category':
        cue = 'Essential'
        column = 'is_essential'
        is_credit_essential = input(f"Is this {type} {cue}? (yes/no): ").lower() == 'yes'    
    date_created = datetime.now()
    created_by = current_user_id
    conn = sqlite3.connect('users.db')
    cur = conn.cursor()
    cur.execute(f'''INSERT INTO {type}s (name, user_description, {column}, date_created, created_by) VALUES (?,?,?,?,?)''', (name, user_description, is_credit_essential, date_created, created_by))
    return name

#Retreive all the values needed to generate 'standard' report

#generate report

#upload record of values used in report to db



#take in a list of options, a 'back, new ect' option and a prompt text
#return either value of the selected option or None
def new_addition_prompt(options, nxt_option, prompt_text,):
    print(prompt_text)
    for i, option in enumerate(options):
        print(f"{i+1}. {options} ")
    print(f"{len(options)+1}. {nxt_option}")
    choice = int(input("Please select an option: "))
    if choice == len(options) + 1:
        new_option = None
        return new_option
    else:
        chosen_option = options[choice - 1]['name']
        return chosen_option


#Report Structure:
#Weekly Report
#pull all records in the weekly date range into a temp table
#one for outgoing
CREATE TEMPORARY TABLE wkly_reporting_out_now AS
SELECT *
FROM m_transactions
JOIN sectors s ON m.sector_id = s.id
WHERE s.is_credit = False;
AND m.date_transaction BETWEEN DATE('now', 'weekday 0', '-7 days') AND DATE('now', 'weekday 0', '-1 day');
#one for income
CREATE TEMPORARY TABLE wkly_reporting_in_now AS
SELECT *
FROM m_transactions
JOIN sectors s ON m.sector_id = s.id
WHERE s.is_credit = True;
AND m.date_transaction BETWEEN DATE('now', 'weekday 0', '-7 days') AND DATE('now', 'weekday 0', '-1 day');
#Query all current info only(stuff that doesnt need old records)
#total out transactions:
now_count = SELECT COUNT(*) AS wkly_entries_total 
FROM wkly_reporting_out_now
#highest out:
now_highest = SELECT date_transaction, processed_description, amount, sector_id
FROM wkly_reporting_out_now 
ORDER BY amount DESC LIMIT 1;
#most common;
SELECT processed_descriptions AS frequency FROM wkly_reporting_out_now ORDER BY frequency DESC LIMIT 1;
#amount up or down vs last week & last year.
last_week_count = SELECT entry_total FROM m_wkly_reporting WHERE lastrowiD 
vs_lastwk = now_count - last_week_count
last_year_count = SELECT entry_total FROM m_wkly_reporting WHERE this week minus 52
vs_lastwk = now_count - last_year_count

#total in: amount
now_in_total = SELECT SUM(amount) as now_in_total
FROM wkly_reporting_in_now
# source breakdown, 
# amount up or down vs last week & last year. 
last_week_in = SELECT in_total FROM m_wkly_reporting WHERE lastrowID
in_vs_lstwk = now_in_total -last_week_in
last_year_in = SELECT in_total FROM m_wkly_reporting WHERE lastrowID - 51
in_vs_lstyr = now_in_total - last_year_in
# Year on year change.
in_yoy = in_vs_lstyr/last_year_in * 100
# #total out: amount,
now_out_total = SELECT SUM(amount) as now_in_total
FROM wkly_reporting_out_now
#amount up or down vs last week & last year. Year on year change.
last_week_out = SELECT out_total FROM m_wkly_reporting WHERE lastrowID
out_vs_lstwk = now_out_total -last_week_out
last_year_out = SELECT out_total FROM m_wkly_reporting WHERE lastrowID - 51
out_vs_lstyr = now_out_total - last_year_out
# Year on year change.
out_yoy = out_vs_lstyr/last_year_out * 100 
# source breakdown, 
# 
#average essentials expenditure: total, 
# vs last week, vs last year, year on year change.
#average non-essential expenditure: total, 
# vs last week, vs last year, year on year change.


#current yearly net position 
ytd_net =  SELECT COALESCE(SUM(net_position), 0) as ytd_net
FROM m_wkly_reporting 
WHERE report_date BETWEEN DATE('now', 'start of year') AND DATE('now')
# net position projections
#average net position * remaining weeks of the year. + current yearly savings
ytd_avg_net = COALESCE(AVG(net_position), 0) as ytd_avg_net
FROM m_wkly_reporting
WHERE report_date BETWEEN DATE('now', 'start of year') AND DATE('now')
current_week = datetime.now().isocalendar()[1]
total_weeks = 52
remaining_weeks = total_weeks - current_week
eoy_net = ytd_avg_net * remaining_weeks if ytd_avg_net is not None else 0 + ytd_net

#current time (weeks) to savings goals
savings goals - total net position since goal added /% average of weekly net  
#current # weeks of essentials banked.
total net position / average weekly essentials 

#Monthly 

