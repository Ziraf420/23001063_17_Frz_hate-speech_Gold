import sqlite3
import pandas as pd
import os


def create_table():
    conn = sqlite3.connect('database.db')
    print("Connected to database successfully")

    conn.execute('CREATE TABLE dirty (Tweet TEXT, HS INT, Abusive INT, HS_Individual INT, HS_Group INT, HS_Religion INT, HS_RACE INT, HS_Physical INT, HS_GENDER INT, HS_OTHER INT, HS_WEAK INT, HS_MODERATE INT, HS_STRONG INT)')
    conn.execute('CREATE TABLE cleaned (Tweet TEXT, HS INT, Abusive INT, HS_Individual INT, HS_Group INT, HS_Religion INT, HS_RACE INT, HS_Physical INT, HS_GENDER INT, HS_OTHER INT, HS_WEAK INT, HS_MODERATE INT, HS_STRONG INT)')
    print("Created table successfully!")

    conn.close()

def dump_data():
    conn = sqlite3.connect('database.db')
    print("Connected to database successfully")

    # dump data.csv into dirty table
    df = pd.read_csv('data.csv')
    df.to_sql('dirty', conn, if_exists='replace', index=False)
    print("Dumped data.csv into dirty table successfully!")

    print("Created table successfully!")
    conn.close()


if __name__ == '__main__':
    if os.path.exists('database.db'):
        # delete
        os.remove('database.db')

    create_table()
    # dump_data()