# call required libraries:
# import pandas for data manipulation
# import numpy for numerical operations
# import matplotlib for plotting
# import sqlite3 for database operations 
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sqlite3
from fuzzywuzzy import fuzz

# on program start:
#connect to user specific database
conn = sqlite3.connect('arthurbudget.db')
cur = conn.cursor()
#check if current db exists, if not create it.
# create table for all transactions (date, name, amount, user description, is income, is expense)
cur.execute('''CREATE TABLE IF NOT EXISTS transactions(id PRIMARY KEY, date DATE, name TEXT, amount REAL, user_description TEXT, is_income INTEGER, is_expense INTEGER, main_category_id INTEGER, subcategory_id INTEGER, niche_category_id INTEGER)''')
# create table for all main categories (name, description, is essential)
cur.execute('''CREATE TABLE IF NOT EXISTS main_categories(ID PRIMARY KEY, name TEXT, description TEXT, is_essential INTEGER)''')
# create table for all subcategories
cur.execute('''CREATE TABLE IF NOT EXISTS subcategories(ID PRIMARY KEY, name TEXT, description TEXT, main_category_id INTEGER)''')
# create a table for niche categories
cur.execute('''CREATE TABLE IF NOT EXISTS niche_categories(ID PRIMARY KEY, name TEXT, description TEXT, subcategory_id INTEGER)''')
# create table for all unique transaction name and category pairs
cur.execute('''CREATE TABLE IF NOT EXISTS unique_transactions(ID PRIMARY KEY, name TEXT, main_category_id INTEGER, subcategory_id INTEGER, niche_category_id INTEGER)''')

#pull list of all unique transaction name and category pairs from main db.
#store in a dataframe for comparison.
df_unique_transactions = pd.read_sql_query("SELECT * FROM unique_transactions", conn)

#prompt user to select file for processing.
current_rocess = userinput("Please select the file you would like to process")
#check if multiple files are selected, if so combine them all into csv file with format (date, description, amount)
# store file in user folder for records. 

#read file into a dataframe
df = pd.read_csv(current_process)
#sort df by alphabetical order of transaction description
df = df.sort_values(by='name')
#compare first transaction description to list of unique transaction name and category pairs.
# if > 90% match, assign category to transaction.


# Assuming 'description' is the column with the strings and 'category' is the column with the categories
for index, row in df.iterrows():
    max_ratio = 0
    max_category = None
    for index_unique, row_unique in df_unique_transactions.iterrows():
        ratio = fuzz.ratio(row['name'], row_unique['description'])
        if ratio > max_ratio:
            max_ratio = ratio
            max_category = row_unique['category']
    if max_ratio > 90:
        df.loc[index, 'category'] = max_category
    else:
        # Prompt user to assign category
        new_category = userinput("Please assign a category to the transaction: " + row['name'])
        df.loc[index, 'category'] = new_category
        # Add to list of unique transaction name and category pairs
        df_unique_transactions = df_unique_transactions.append({'description': row['name'], 'category': new_category}, ignore_index=True)
# If no match, prompt user to assign category to transaction. 
# If user assigns new category add to list of unique transaction name and category pairs.

#repeat for all transactions in df until all transactions are categorized.

#once categorised add df to main db. 








