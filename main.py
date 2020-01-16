from datetime import datetime
from flask import Flask, jsonify, request, render_template
from elasticsearch import Elasticsearch
import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import seaborn as sns
import matplotlib.pyplot as plt
import re, email, json
import requests
import dateutil.parser

populate_elastic = False
es = Elasticsearch()
app = Flask(__name__)

fields = ['Message_ID', 'From', 'To', 'Subject', 'Content_Type', 'content', 'user', 'insert_time']
		# 'X_From', 'X_To', 'X_Cc', 'X_Bcc', 'X_Folder', 'X_Origin', 'X_FileName', 'Mime_Version', 'Content_Transfer_Encoding']

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
		'From': mfrom,
		'To': mto,
		'Subject': msubject,
		'Content_Type': mctype,
		'content': mcontent,
		'user': muser,
		'insert_time': datetime.now(),
		'spam': data['spam']
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
	keyword = request.form['searchbar']

	line = re.findall(r"Date:\[.*\]", keyword)
	if line != []:
		line = line[0][6:-1].split(' TO ')
		if line[0] == '*':
			start_date = unix_time_millis(datetime.utcfromtimestamp(0))
		else:
			start_date = unix_time_millis(dateutil.parser.parse(line[0]))


		if line[1] == '*': 
			end_date = unix_time_millis(datetime.now())
		else:
			end_date = unix_time_millis(dateutil.parser.parse(line[1]))

		print('Date:[' + line[0] + ' TO ' + line[1] + ']')
		keyword = keyword.replace('Date:[' + line[0] + ' TO ' + line[1] + ']', 
			'Date:[' + str(int(start_date)) + ' TO ' + str(int(end_date)) + ']')

	print('query is:', keyword)
	res = es.search(index="emails", doc_type="email", q=keyword)

	return jsonify(res['hits']['hits'])

@app.route('/conversations', methods=['GET'])
def conversations():
	global fields
	return render_template('conversations.html',thing_to_say='hello', fields=fields)


@app.route('/conversations', methods=['POST'])
def search_conv():
	res = es.search(index="test3_conv")
	return jsonify(res['hits']['hits'])

@app.route('/conversations_by_id', methods=['POST'])
def conversations_by_id():
	keyword = request.form['message_id_search']
	res = es.get(index="test3_conv", doc_type='conversation', id=keyword)
	return jsonify(res)

@app.route('/conversations_by_message_content', methods=['POST'])
def conversations_by_message_content():
	keyword = request.form['search_message_content']
	body = {
		"query" : {
			"multi_match" : {
		        "fields": ["conversation"],
		        "query" : keyword

			}
		} 
	}

	res = es.search(index="test3_conv",  q=keyword)
	return jsonify(res['hits']['hits'])



def unix_time_millis(dt):
	epoch = datetime.utcfromtimestamp(0)
	return (dt - epoch).total_seconds() * 1000

def ready_to_insert(nr_emails=500):
	global populate_elastic
	if populate_elastic:
		
		bodies = []
		chunk = pd.read_csv('emails.csv', chunksize=500)
		for i in range(int(nr_emails / 500)):
			print('chunk', i)
			hamspam = joblib.load('first100k')
			
			pd.options.mode.chained_assignment = None
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
			emails_df['Date'] = emails_df['Date'].apply(lambda t: pd.Timestamp(t[:-12])).values

			# Extract the root of 'file' as 'user'
			emails_df['user'] = emails_df['file'].map(lambda x:x.split('/')[0])
			emails_df['spam'] = hamspam[i * 500: (i + 1) * 500]

			del messages

			for j in range(i * 500, (i + 1) * 500):
				mydata = emails_df.loc[j].to_json()
				bodies.append(build_body(mydata))

		for body in bodies:
			if body['spam']:
				print('spam')
			result = es.index(index='emails', doc_type='email', id=body['Message_ID'], body=body)
	return "ok"


def create_conversaiton_index(nr_emails=500):	
	global populate_elastic
	if populate_elastic:
		
		original_messages = []
		messages_list = []
		bodies = []
		chunk = pd.read_csv('emails.csv', chunksize=nr_emails)
		# for i in range(int(nr_emails / 500)):
			# print('chunk', i)

		emails_df = next(chunk)

		messages = list(map(email.message_from_string, emails_df['message']))
		emails_df.drop('message', axis=1, inplace=True)
		# Get fields from parsed email objects
		keys = messages[0].keys()
		for key in keys:
			emails_df[key] = [doc[key] for doc in messages]
		# Parse content from emails
		emails_df['content'] = list(map(get_text_from_email, messages))
		for j in range(nr_emails):
			fdata = emails_df.loc[j]
			if ('-----Original Message-----' not in fdata['content']):
				original_messages.append(
					{
						"message_content" : fdata['content'],
						"message_id" : fdata['Message-ID'],
						"conversation" : []
					}
					)

		for k in range(nr_emails):
			fdata = emails_df.loc[k]
			if ("-----Original Message-----" in fdata['content']):
				for message in original_messages:
					if message['message_content'] in fdata['content']:
						message['conversation'].append(fdata['content'])

		for conv in original_messages:
			if conv['conversation'] : 
				print ('this is a conversation')
				result = es.index(index='test3_conv', doc_type='conversation', id=conv['message_id'], body=conv)

		print('I am printing a shite')
		for mess in original_messages:
			if conv['conversation'] :  
				print('~~ ' + mess['message_content'] + ' **** ' + mess['conversation'] + ' ###')

		print('I have printed all the shite')
		# print ("khart ")
		# print (len(original_messages))

import glob
import joblib
if __name__ == "__main__":
	ready_to_insert(nr_emails=10000)
	create_conversaiton_index(nr_emails = 10000)
	print("stated shite")
	app.run(port=5000, debug=True)