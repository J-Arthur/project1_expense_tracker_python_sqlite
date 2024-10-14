
import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
import datetime

def on_ready():
    if not os.path.exists('users.db'):
        conn = sqlite3.connect('users.db')
        cur = conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS users (ID, name TEXT, year_income REAL, hourly_rate REAL, estimated_weekly_hours REAL)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS m_transactions (ID, date TEXT, description TEXT, amount REAL, category TEXT, 
            sub_category TEXT, niche TEXT, sector TEXT, user_description TEXT, is_credit INTEGER, is_essential INTEGER)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS sectors (ID, title TEXT, user_description TEXT, is_credit INTEGER, u_limit REAL)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS categories (ID, title TEXT, user_description TEXT, is_credit INTEGER, is_essential INTEGER, u_limit REAL)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS sub_categories (ID, title TEXT, user_description TEXT, is_credit INTEGER, is_essential INTEGER, u_limit REAL)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS niche (ID, title TEXT, user_description TEXT, is_credit INTEGER, is_essential INTEGER, u_limit REAL)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS budget_running_summary (ID, year INTEGER, month INTEGER, number_transactions INTEGER, total_in REAL, total_out REAL, total_unique_descriptions INTEGER,)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS budget_history_summary (ID, year INTEGER, month INTEGER, number_transactions INTEGER, total_in REAL, total_out REAL, total_unique_descriptions INTEGER)''')
        #cur.execute('''CREATE TABLE IF NOT EXISTS budget (ID, year INTEGER, month INTEGER, number_transactions INTEGER, total_in REAL, total_out REAL, total_unique_descriptions INTEGER)''')
        cur.commit()
        conn.close()
        create_new_user()
    else:
        main_menu()

def create_new_user():
     name = input("Please enter your name: ")
     if input("Do you know your expected annual income? (y/n) ") == 'y':
         year_income = input("Please enter your expected annual income: ")
         hourly_rate = None
         estimated_weekly_hours = None
     else:
            hourly_rate = input("Please enter your hourly rate: ")
            estimated_weekly_hours = input("Please enter your estimated weekly hours: ")
            year_income = hourly_rate * estimated_weekly_hours * 52
            conn = sqlite3.connect('users.db')
            cur = conn.cursor()
            cur.execute('''INSERT INTO users (name, year_income, hourly_rate, estimated_weekly_hours) VALUES (?,?,?,?)''', (name, year_income, hourly_rate, estimated_weekly_hours))
            cur.commit()
            conn.close()
            create_new_budget()

def create_new_budget():
    conn = sqlite3.connect('users.db')
    cur = conn.cursor()
    cur.execute('''SELECT name FROM users WHERE ID = (SELECT MAX(ID) FROM users)''')
    name = cur.fetchone()
    print(f"You are starting a new budget on {datetime.datetime.now()} as {name}")
    update_frequency = input("Please enter how often you would like to update your budget (daily, weekly, monthly): ")
    cur.execute('''INSERT INTO budget_running_summary (year, month, number_transactions, total_in, total_out, total_unique_descriptions) VALUES (?,?,?,?,?,?)''', (datetime.datetime.now().year, datetime.datetime.now().month, 0, 0, 0, 0))
    cur.commit()
    conn.close()
    if input("Would you like to set some savings goals? (y/n) ") == 'y':
        set_savings_goals()
    else:
        print("We are going to get you to process some transactions to get started. You will need to categorise these manually to begin with but the longer you use this the fewer transactions you will need to manually adjust.")
        process_transactions()
    
def process_transactions():
    conn = sqlite3.connect('users.db')
    cur = conn.cursor()
    cur.execute('''SELECT description, category, sub_category, niche, sector FROM m_transactions''')
    unique_pairs = cur.fetchall()
    unique_pairs = pd.DataFrame(unique_pairs, columns=['description', 'category', 'sub_category', 'niche', 'sector'])
    
    # Pull all categories into lists
    categories = [row[0] for row in cur.execute('''SELECT title FROM categories''').fetchall()]
    subcategories = [row[0] for row in cur.execute('''SELECT title FROM sub_categories''').fetchall()]
    niches = [row[0] for row in cur.execute('''SELECT title FROM niche''').fetchall()]
    sectors = [row[0] for row in cur.execute('''SELECT title FROM sectors''').fetchall()]
    transactions = pd.read_csv(file_path)
    print(transactions.head())
    preprocess_transactions(unique_pairs,transactions)
    
    for index, row in transactions.iterrows():
        description = row['description']
        matches = process.extract(description, unique_pairs['description'], scorer=fuzz.token_sort_ratio)
        
        # Filter matches with similarity > 90%
        high_confidence_matches = [match for match in matches if match[1] > 90]
        
        if len(high_confidence_matches) == 1:
            # Single high confidence match
            matched_description = high_confidence_matches[0][0]
            match = unique_pairs[unique_pairs['description'] == matched_description]
            transactions.at[index, 'category'] = match.iloc[0]['category']
            transactions.at[index, 'sub_category'] = match.iloc[0]['sub_category']
            transactions.at[index, 'niche'] = match.iloc[0]['niche']
            transactions.at[index, 'sector'] = match.iloc[0]['sector']
        elif len(high_confidence_matches) > 1:
            # Multiple high confidence matches
            print(f"Multiple high confidence matches found for '{description}':")
            for i, match in enumerate(high_confidence_matches):
                print(f"{i + 1}. {match[0]} (Similarity: {match[1]}%)")
            
            choice = int(input("Please select the correct match (enter the number) or 0 to assign manually: "))
            if choice > 0 and choice <= len(high_confidence_matches):
                matched_description = high_confidence_matches[choice - 1][0]
                match = unique_pairs[unique_pairs['description'] == matched_description]
                transactions.at[index, 'category'] = match.iloc[0]['category']
                transactions.at[index, 'sub_category'] = match.iloc[0]['sub_category']
                transactions.at[index, 'niche'] = match.iloc[0]['niche']
                transactions.at[index, 'sector'] = match.iloc[0]['sector']
            else:
                # Assign manually
                category, sub_category, niche, sector, unique_pairs = assign_category(description, unique_pairs, categories, sub_categories, niches, sectors)
                transactions.at[index, 'category'] = category
                transactions.at[index, 'sub_category'] = sub_category
                transactions.at[index, 'niche'] = niche
                transactions.at[index, 'sector'] = sector
        else:
            # No high confidence match
            category, sub_category, niche, sector, unique_pairs = assign_category(description, unique_pairs, categories, sub_categories, niches, sectors)
            transactions.at[index, 'category'] = category
            transactions.at[index, 'sub_category'] = sub_category
            transactions.at[index, 'niche'] = niche
            transactions.at[index, 'sector'] = sector
    
    print(transactions.head())
    return transactions, unique_pairs
    
def preprocess_transactions(unique_pairs, transactions):
    transactions['processed_description'] = transactions['description']
    transactions['processed_description'] = transactions['processed_description'].str.replace(' ', '')
    transactions['processed_description'] = transactions['processed_description'].str.replace('[^a-zA-Z]', '')
    transactions['processed_description'] = transactions['processed_description'].str.ljust(unique_pairs['description'].str.len().max(), fillchar=' ')
    transactions['amount'] = transactions['amount'].astype(float)
    transactions['date'] = pd.to_datetime(transactions['date'])
    transactions.to_csv(f'{datetime.datetime.now()}_preprocessed_transactions.csv')
    return transactions

def assign_category(description, unique_pairs, categories, sub_categories, niches, sectors):
        def prompt_user(options, prompt_text):
            print(prompt_text)
        for i, option in enumerate(options):
            print(f"{i + 1}. {option}")
        print(f"{len(options) + 1}. Add new")
        choice = int(input("Please select an option: "))
        if choice == len(options) + 1:
            new_option = input("Enter new option: ")
            return new_option, True
        else:
            return options[choice - 1], False

        category, is_new_category = prompt_user(categories, "Select category:")
        if is_new_category:
            is_essential = input("Is this category essential? (yes/no): ").lower() == 'yes'
        categories.append(category)
    
        sub_category, is_new_sub_category = prompt_user(sub_categories, "Select sub category:")
        if is_new_sub_category:
        is_essential = input("Is this sub category essential? (yes/no): ").lower() == 'yes'
        sub_categories.append(sub_category)
        elif sub_category.lower() == "no sub category":
        sub_category = "no sub category"
    
        niche, is_new_niche = prompt_user(niches, "Select niche:")
        if is_new_niche:
        is_essential = input("Is this niche essential? (yes/no): ").lower() == 'yes'
        niches.append(niche)
        elif niche.lower() == "no niche":
        niche = "no niche"
    
        sector, is_new_sector = prompt_user(sectors, "Select sector:")
        if is_new_sector:
        is_credit = input("Is this sector a credit sector? (yes/no): ").lower() == 'yes'
        sectors.append(sector)
    
        user_description = input("Enter user description: ")
    
    # Add to unique_pairs
    unique_pairs = unique_pairs.append({
        'description': description,
        'category': category,
        'sub_category': sub_category,
        'niche': niche,
        'sector': sector
    }, ignore_index=True)
    
return category, sub_category, niche, sector, unique_pairs
# create_new_budget function:
#     - get current date and time. (You are starting a new budget on **date** at **time**)
#     - ask user for budget update frequency (for notification purposes)
#     - create budget entry in budget table. 
#     - ask user for expected income for calendar year or hourly rate and estimated weekly hours.
#         - if hourly rate and estimated weekly hours then calculate estimated annual income. 
#     - ask user if they would like to set some savings targets. if yes call "set_savings_goals" function. 
#     - ("we are going to get you to process some transactions to get started. You will need to categorise these manually to begin with but the longer you use this the fewer transactions you will need to manually adjust.")
#     - call "process_transactions" function. 
#     - once first transactions are processed ask user if they would like to "backdate" their expenditure by processing previous transactions. 
#     - if yes notify them ("""This will give you very valuable insight into your spending habits. But it will take
#                           some time to process. Are you sure you want to do this?(Do you have up to a couple hours to spare?)""")
#     (""" You will need to manually categorise a large number of transactions. As these could be well in the past if you are unsure if there was a specific niche for a purchase just categorise as clearly as possible I.E, gorcery purchase that MIGHT have been for a dinner party rather than regular 
#      groceries. Just leave it without a niche. You can always go back and adjust it later.""") 
#     (""" The easiest way to do this is to download a csv file of your transactions going as far back as possible rather than monthly.""")
    
    
# main menu:
#     1. process transactions
#     2. view current budget
#     3. view reports
#     4. edit budget
#     5. view all stored transactions
    
# option 1; process transactions
# - ask user for the file path of the transactions (like the save as type file search)
# - read file into dataframe. 
# - ask user if any additional files need to be processed. if yes repeat the above steps. if no call "preprocess_transactions" function
# - merge_transaction_files function:
#     - Identify the columns in the dataframe that contain the date, description, amount
#     - confirm with user that column assignment is correct. if not ask for the correct column name from options
#     - add columns for category, sub category, niche, sector, user description, is credit, is essential
#     - copy description column to non user visible column "processed_description"
#     - remove whitespace, special characters and numbers from the "processed_description" column
#     - convert the amount column to a float
#     - convert the date column to a datetime object
#     - save preprocessed data to new csv file labled as todays date and time.
#     - aske user to review transactions and allow for manual editing of the data. (scroll to the bottom and hit "no edits, continue")
#     - call "process_transactions" function
    
# - preprocess_transactions function:
#     add a new column to the transactions dataframe called "processed_description". Copy the description column to the processed description column.
#     remove any whitespace from the processed_description column. 
#     remove special characters (/,.-!?;:) from the processed_description column 
#     remove numbers from the processed_description column
#     find processed_description with most characters from unique_pairs.df add whitespace to right hand side of all descriptions to match the length of the longest processed_description.
#     convert the amount column to a float
#     convert the date column to a datetime object
#     save preprocessed data to new csv file labled as todays date and time in same filepath as user gave.
#     return preprocessed transactions dataframe
    

# - process_transactions function:
#     - pull all unique description and category pairs from the database into a dataframe
#     - pull all categories into a list
#     - pull all sub categories into a list
#     - pull all niche into a list
#     - itterate through the transactions dataframe and compare the processed descriptions with the unique_pairs df.
#     - if >90% match then assign the category, sub category, niche, sector to transaction. move on to next transaction.
#     - if >90% match on multiple then display both WITH user description and ask user to select the correct one.
#     - if no match then pause itteration and call "assign_category" function
# - assign_category function: 
#     - display transaction and ask user to assign sector, prompt list of sectors or "new sector"
#         -if new sector then ask user for sector name and if it is a credit or debit sector.
#     - prompt user for category, prompt list of categories or "new category"
#         -if new category then ask user for category name and if it is essential.
#     - prompt user for sub category, prompt list of sub categories or "new sub category" or "no sub category"
#         -if new sub category then ask user for sub category name and if it is essential.
#         - if no sub category then assign sub category as "no sub category"
#     - prompt user for niche, prompt list of niche or "new niche" or "no niche"
#         -if new niche then ask user for niche name and if it is essential.
#         - if no niche then assign niche as "no niche"
#     - prompt user for user description
#     - if description category combination is unique then add to the unique description category dataframe
#     - exit function and return to process_transactions function
    
#      - continue itteration through transactions dataframe until all transactions are assigned categories. 
#      - display processed transactions for user review and allow for manual editing and adding user descriptions.
#      - commit processed transactions to database. 
#         - call "update_budget_summary" function.
        
# - update_budget_summary function:
#     - pull all transactions for the current month into a dataframe
#     - calculate the total in and total out for the month
#     - calculate the number of unique transaction descriptions
#     - calculate the number of transactions
#     - calculate total amount of each category, sub category, niche, sector.
#     - append the data to the current_budget summary table
#     - exit function
    
#  - tell user "here is **current months** progress" call "view_current_budget" function.
 
 
#   - option 2: view current budget
#     - call "view_current_budget" function
# - view_current_budget function:
#     - pull the current months budget summary from the database
#     - pull "cuurent month budge goals" from the database
#     - display the budget summary to the user
#     - display the budget summary in a pie chart
#     - display the budget summary in a bar chart
#     - show "time to savings goal" estimates. 
    
#     - exit function
    
    