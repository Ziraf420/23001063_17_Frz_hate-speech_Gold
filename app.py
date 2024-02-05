from flask import Flask, request
from flask_restx import Api, Resource, fields
import pandas as pd
import create_table as ct
import cleaner as cl
import sqlite3
import io
from werkzeug.datastructures import FileStorage

app = Flask(__name__)
api = Api(app, version='1.0', title='Data Cleaning API',
    description='A simple API to clean ',
)

# ct.create_table()

single_ns = api.namespace('single', description='a single operation')
single_parser = api.parser()
single_parser.add_argument('tweet', type=str, required=True, help='Tweet to be cleaned')


def cleanTweet(text):
    text = cl.Cleaner.lowercase(text)
    text = cl.Cleaner.remove_nonaplhanumeric(text)
    text = cl.Cleaner.remove_unnecessary_char(text)
    text = cl.Cleaner.normalize_alay(cl.Cleaner.alay_dict, text)
    text = cl.Cleaner.stemming(cl.Cleaner.stemmer, text)
    text = cl.Cleaner.remove_stopword(cl.Cleaner.id_stopword_dict, text)
    text = cl.Cleaner.remove_unicode(text)
    text = cl.Cleaner.remove_extra_spaces(text)
    return text

@single_ns.route('/create')
@single_ns.expect(single_parser)
class SingleCreate(Resource):
    def post(self):
        args = single_parser.parse_args()
        tweet = args['tweet']

        cleaned_tweet = cleanTweet(tweet)
        with sqlite3.connect('database.db') as conn:
            cursor = conn.cursor()
            cursor.execute('INSERT INTO dirty (Tweet) VALUES (?)', (tweet,))
            cursor.execute('INSERT INTO cleaned (Tweet) VALUES (?)', (cleaned_tweet,))
            conn.commit()

            cursor.close()
            return {'message': 'Tweet created successfully',
                    'dirty': tweet,
                    'cleaned': cleaned_tweet,
                    }, 201

@single_ns.route('/list-dirty')
class SingleList(Resource):
    def get(self):
        with sqlite3.connect('database.db') as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM dirty')
            data = cursor.fetchall()
            cursor.close()
            return {'data': data}, 200
        
@single_ns.route('/list-cleaned')
class SingleList(Resource):
    def get(self):
        with sqlite3.connect('database.db') as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM cleaned')
            data = cursor.fetchall()
            cursor.close()
            return {'data': data}, 200


batch_ns = api.namespace('batch', description='batch csv operations')

batch_parser = api.parser()
batch_parser.add_argument('csv_file', type=FileStorage, location='files', required=True, help='CSV file to be cleaned')

@batch_ns.route('/csv')
@batch_ns.expect(batch_parser)
class CsvUpload(Resource):
    def post(self):
        args = batch_parser.parse_args()
        csv_file = args.get('csv_file')

        print(csv_file)

        # save csv file to disk
        csv_file.save('temp.csv')

        # read filestorage csv file into pandas dataframe
        df = pd.read_csv('temp.csv', encoding = "ISO-8859-1")

        df_cleaned = df.copy()
        df_cleaned['Tweet'] = df_cleaned['Tweet'].apply(cleanTweet)
    

        with sqlite3.connect('database.db') as conn:
            df.to_sql('dirty', conn, if_exists='replace', index=False)
            df_cleaned.to_sql('cleaned', conn, if_exists='replace', index=False)
            conn.commit()

        return {'message': 'CSV uploaded and cleaned successfully',
                'dirty_data': df.to_dict(orient='records'),
                'cleaned_data': df_cleaned.to_dict(orient='records'),
                }, 201

if __name__ == '__main__':
    app.run(debug=True)

