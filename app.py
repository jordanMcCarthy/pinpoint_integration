import sys
import os
from flask import Flask
from flask import request, jsonify
from flask_restful import reqparse, Api, Resource
import json
import pyodbc
from datetime import date, datetime

# Initialize Flask
app = Flask(__name__)

# Setup Flask Restful framework
api = Api(app)
parser = reqparse.RequestParser()
parser.add_argument('activity')

# Create connection to Azure SQL
conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server=tcp:pinpoint-integration.database.windows.net,1433;Database=statseek_pinpoint;Uid=jordan;Pwd=Bosco9097seinfeld;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
print("connected")

def dictFactory(cursor, row):
    """
    Function that parses the entries of the database and returns them as a list of dictionaries.
    @param cursor -- A cursor object using sqlite.
    @param row -- The row of the database being parsed.
    """
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

@app.route('/', methods=['GET'])
def homePage():
    return '''
    <h1>Datascience Jobs Database</h1>
    <h3>You have reached: /home/</h3>
    <p>To view all entries in the database: '127.0.0.1:5000/api/v1/jobs/datascience/all' </p>
    <p>To filter entries based on country : '127.0.0.1:5000/api/v1/jobs/datascience?country=United%20States' </p>
    <p>To filter entries based on post id : '127.0.0.1:5000/api/v1/jobs/datascience?id=81953194' </p>
'''

@app.route('/activity/all', methods=['GET'])
def apiViewAll():
 # Create connection to Azure SQL
    conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server=tcp:pinpoint-integration.database.windows.net,1433;Database=statseek_pinpoint;Uid=jordan;Pwd=Bosco9097seinfeld;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    print("connected")
    cursor = conn.cursor()
    result = cursor.execute('SELECT * FROM [dbo].[Activity]').fetchall()
    items = [dict(zip([key[0] for key in cursor.description], row)) for row in result]
    return jsonify(items)


@app.route('/activity', methods=['GET'])
def apiViewByFilter():
    '''
    Function that allows users to filter the results in the API based on specified input.
    '''
    query_parameters = request.args

    id = query_parameters.get('id')
    lastModified = query_parameters.get('lastModified')
    activity = query_parameters.get('activity')
    customer = query_parameters.get('customer')

    query = "SELECT * FROM [dbo].[Activity] WHERE"
    to_filter = []

    if id:
        query += ' [ActivityID]=? AND'
        to_filter.append(id)

    if lastModified:
        query += ' [LastModified]>? AND'
        to_filter.append(lastModified)

    if activity:
        query += ' [Activity]=? AND'
        to_filter.append(activity)

    if customer:
        query += ' [Customer]=? AND'
        to_filter.append(customer)

    query = query[:-4] + ';'


    conn = pyodbc.connect(
        'Driver={ODBC Driver 17 for SQL Server};Server=tcp:pinpoint-integration.database.windows.net,1433;Database=statseek_pinpoint;Uid=jordan;Pwd=Bosco9097seinfeld;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    print("connected")
    cursor = conn.cursor()
    result = cursor.execute(query, to_filter).fetchall()
    items = [dict(zip([key[0] for key in cursor.description], row)) for row in result]
    return jsonify(items)

app.run()