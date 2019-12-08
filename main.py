from datetime import datetime
from flask import Flask, jsonify, request, render_template
from elasticsearch import Elasticsearch
import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import seaborn as sns
import matplotlib.pyplot as plt
import re, email, json
import requests
import dateparser

populate_elastic = True
es = Elasticsearch()
app = Flask(__name__)

fields = ['Message_ID', 'From', 'To', 'Subject', 'Mime_Version', 'Content_Type',
        'Content_Transfer_Encoding', 'X_From', 'X_To', 'X_Cc', 'X_Bcc', 'X_Folder',
        'X_Origin', 'X_FileName', 'content', 'user', 'insert_time']

## Helper functions
def get_text_from_email(msg):
    '''To get the content from email objects'''
    parts = []
    for part in msg.walk():
        if part.get_content_type() == 'text/plain':
            parts.append( part.get_payload() )
    return ''.join(parts)

def split_email_addresses(line):
    '''To separate multiple email addresses'''
    if line:
        addrs = line.split(',')
        addrs = frozenset(map(lambda x: x.strip(), addrs))
    else:
        addrs = None
    return addrs

def parseDate(date) :
	date = date.split(',')[1]
	date = date.split('(')[0]
	parsedDate = dateparser.parse(date)
	return parsedDate

def build_body(data):
    global populate_elastic

    if populate_elastic:
        data = json.loads(data)

    mid = data['Message-ID']
    mdate = data['Date']
    mfrom = data['From']
    mto = data['To']
    msubject = data['Subject']
    mmime = data['Mime-Version']
    mctype = data['Content-Type']
    mcte = data['Content-Transfer-Encoding']
    mxfrom = data['X-From']
    mxto = data['X-To']
    mxcc = data['X-cc']
    mxbcc = data['X-bcc']
    mxfolder = data['X-Folder']
    mxorigin = data['X-Origin']
    mxfilename = data['X-FileName']
    mcontent = data['content']
    muser = data['user']

    body = {
        'Message_ID': mid,
        'Date': mdate,
        'ParsedDate' : parseDate(mdate),
        'From': mfrom,
        'To': mto,
        'Subject': msubject,
        'Mime_Version': mmime,
        'Content_Type': mctype,
        'Content_Transfer_Encoding': mcte,
        'X_From': mxfrom,
        'X_To': mxto,
        'X_Cc': mxcc,
        'X_Bcc': mxbcc,
        'X_Folder': mxfolder,
        'X_Origin': mxorigin,
        'X_FileName': mxfilename,
        'content': mcontent,
        'user': muser,
        'insert_time': datetime.now()
    }

    return body

@app.route('/', methods=['GET'])
def index():
    global fields
    return render_template('home.html',thing_to_say='hello', fields=fields)

@app.route('/delete', methods=['GET'])
def delete():
    results = es.indices.delete(index='emails', ignore=[400, 404])
    return jsonify(results)

@app.route('/insert_data', methods=['POST'])
def insert_data():
    data = request.json
    body = build_body(data)
    result = es.index(index='emails', doc_type='email', id=body['Message_ID'], body=body)

    return jsonify(result)

@app.route('/search', methods=['POST'])
def search():
    keyword = request.json['keyword']

    body = {
        "query": {
            "multi_match": {
                "query": keyword,
                "fields": ["content", "user", "Subject", "From", "To"]
            }
        }
    }

    res = es.search(index="emails", doc_type="email", body=body)

    return jsonify(res['hits']['hits'])

@app.route('/search_fields', methods=['POST'])
def search_fields():
    keyword = request.form['searchbar']
    print(keyword)

    local_fields = [key for key in fields if key in request.form.keys() and request.form[key] == 'on']
    local_dates = [request.form[key+'Text'] if key in request.form.keys() and request.form[key] == 'on' else None for key in ['startDate', 'endDate']]
    
    start_date = local_dates[0]
    end_date = local_dates[1]

    if start_date is None:
        start_date = datetime.utcfromtimestamp(0)
        start_date = start_date.strftime('%Y-%m-%dT%H:%M:%S')

    if end_date is None: 
        end_date = datetime.now()
        end_date = end_date.strftime('%Y-%m-%dT%H:%M:%S')

    if local_fields == []:
        body = {
            "query": {
                "match_all" : {}
            }
        }
    else:
        body = {
            "query": {
                "bool": {
                    "filter": [{
                        "multi_match": {
                            "query": keyword,
                            "fields": local_fields
                        }
                    },
                    {
                        "range": {
                            "ParsedDate": {
                                "gte": start_date,
                                "lte": end_date,
                                "format": 'yyyy-MM-dd\'T\'HH:mm:ss'
                            }
                        }
                    }]
                }
            }
        }

    print(body)
        

    res = es.search(index="emails", doc_type="email", body=body)

    return jsonify(res['hits']['hits'])

def ready_to_insert():
    global populate_elastic
    if populate_elastic:
        pd.options.mode.chained_assignment = None
        chunk = pd.read_csv('emails.csv', chunksize=500)
        emails_df = next(chunk)

        # Parse the emails into a list email objects
        messages = list(map(email.message_from_string, emails_df['message']))
        emails_df.drop('message', axis=1, inplace=True)
        # Get fields from parsed email objects
        keys = messages[0].keys()
        for key in keys:
            emails_df[key] = [doc[key] for doc in messages]
        # Parse content from emails
        emails_df['content'] = list(map(get_text_from_email, messages))
        # Split multiple email addresses
        emails_df['From'] = emails_df['From'].map(split_email_addresses)
        emails_df['To'] = emails_df['To'].map(split_email_addresses)

        # Extract the root of 'file' as 'user'
        emails_df['user'] = emails_df['file'].map(lambda x:x.split('/')[0])
        del messages

        for i in range(0, len(emails_df)):
            mydata = emails_df.loc[i].to_json()
            body = build_body(mydata)
            result = es.index(index='emails', doc_type='email', id=body['Message_ID'], body=build_body(mydata))

        return jsonify("Populated successfully")

    return jsonify("Hello World. Make populate_elastic True in order to populate elastic, then restart app, then hit localhost:5000/ again")

if __name__ == "__main__":
    app.run(port=5000, debug=True)