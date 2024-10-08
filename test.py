import sqlite3
import pandas as pd
import os
print("Hello World")


def on_ready():
    if not os.path.exists('users.db'):
        conn = sqlite3.connect('users.db')
        cur = conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS users (ID, name TEXT, year_income REAL, hourly_rate REAL, estimated_weekly_hours REAL)''')
        print("Db created")
    else:
        print("Db already exists")

on_ready()