# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""
import time

import threading
from apps.home import blueprint
from flask import Flask, render_template, request, jsonify
from flask_login import login_required
from jinja2 import TemplateNotFound
from flask import Flask, render_template
import happybase
import plotly.graph_objs as go
import numpy as np

state_attribute = 0
state_attribute2 = 0
def update_state():
    global state_attribute
    global state_attribute2
    i=0
    while True:
        i+=1
        # Update state
        state_attribute = i
        state_attribute2 = i
        print("updated")
        time.sleep(3)
@blueprint.route('/state')
def get_state():

    connection = happybase.Connection(host='localhost', port=9090)
    table = connection.table('status')
    data = {}
    for key, value in table.scan():
        gender = key.decode('utf-8')  # Convert bytes to string
        count = int(value[b'count:count'].decode('utf-8'))  # Convert bytes to integer
        data[gender] = count
    labelStatus = list(data.keys())
    countStatus = list(data.values())
    print(labelStatus)
    print(countStatus)

    return jsonify(countStatus)

@blueprint.route('/index')
@login_required
def index():
    # threading.Thread(target=update_state).start()
    connection = happybase.Connection(host='localhost', port=9090)
    connection.open()

    table = connection.table('genders')

    # Collect data from table.scan()
    data = {}
    for key, value in table.scan():
        gender = key.decode('utf-8')  # Convert bytes to string
        count = int(value[b'count:number'].decode('utf-8'))  # Convert bytes to integer
        data[gender] = count

    # Extract labels and counts from the collected data
    labels = list(data.keys())
    counts = list(data.values())
    totalPatients = np.sum(counts)
    table = connection.table('age')

    # Collect data from table.scan()
    data = {}
    for key, value in table.scan():
        age_group = key.decode('utf-8')  # Convert bytes to string
        count = int(value[b'count:number'].decode('utf-8'))  # Convert bytes to integer
        data[age_group] = count

    # Extract labels and counts from the collected data
    labelsAge = list(data.keys())
    countsAge = list(data.values())
    table = connection.table('MedicalCondition')

    data = {}
    for key, value in table.scan():
        condition = key.decode('utf-8')  # Convert bytes to string
        count = int(value[b'count:number'].decode('utf-8'))  # Convert bytes to integer
        data[condition] = count

    # Extract labels and counts from the collected data
    labelsCondition = list(data.keys())
    countsCondition = list(data.values())
    table = connection.table('bloodtype')  # Replace 'your_table_name' with the actual table name

    # Initialize empty dictionaries to store data
    data = {}
    blood_types = set()

    # Collect data from table.scan()
    for key, value in table.scan():
        blood_type = key.decode('utf-8')  # Assuming the row key represents the blood type
        count = int(value[b'count:number'].decode('utf-8'))  # Extract count
        data[blood_type] = count
        blood_types.add(blood_type)

    # Sort blood types for consistent plotting order
    bloodTypes = sorted(blood_types)

    # Prepare data for plotting
    bloodTypesCounts = [data[blood_type] for blood_type in blood_types]
    table = connection.table('billingbyHospital')  # Replace 'your_table_name' with the actual table name

    # Initialize empty dictionaries to store data
    data = {}

    # Collect data from table.scan()
    for key, value in table.scan():
        provider = key.decode('utf-8')  # Assuming the row key represents the insurance provider

        # Check if the 'count:number' key exists in the value dictionary
        if b'count:number' in value:
            count = float(value[b'count:number'].decode('utf-8'))  # Extract count as float
            data[provider] = count
        else:
            print(f"Warning: 'count:number' key not found for {provider}")

    # Sort providers for consistent plotting order
    providers = sorted(data.keys())

    # Prepare data for plotting
    countsProviders = [data[provider] for provider in providers]
    sumTotalBills = np.sum(countsProviders)

    table = connection.table('MedicalConditiongender')

    # Assuming you have connected to HBase and opened the table as 'table' already

    # Initialize empty dictionaries to store data
    data = {}
    genders = set()
    conditions = set()

    # Collect data from table.scan()
    for key, value in table.scan():
        gender = key.decode('utf-8')  # Assuming the row key represents the gender
        if gender not in data:
            data[gender] = {}
            genders.add(gender)

        # Extract health conditions and their values from the value dictionary
        for column, val in value.items():
            condition = column.decode('utf-8').split(':')[0]  # Extract health condition name
            count = int(val.decode('utf-8'))  # Convert count to integer
            data[gender][condition] = count
            conditions.add(condition)

    # Sort genders and conditions for consistent plotting order
    genders = sorted(genders)
    conditions = sorted(conditions)

    dataset= {gender: [data[gender].get(condition, 0) for condition in conditions] for gender in genders}


    table = connection.table('MedicalConditionByAge')

    # Assuming you have connected to HBase and opened the table as 'table' already

    # Initialize empty dictionaries to store data
    data = {}
    age_groups = set()
    conditions2 = []

    # Collect data from table.scan()
    for key, value in table.scan():
        row_key = key.decode('utf-8')  # Convert row key to string
        age_group = row_key  # Assuming row key is directly the age group

        # Extract health conditions and their values from the value dictionary
        for column, val in value.items():
            condition = column.decode('utf-8').split(':')[0]  # Extract health condition name
            count = int(val.decode('utf-8'))  # Convert count to integer

            # Initialize the data dictionary for the age group if not already present
            if age_group not in data:
                data[age_group] = [0] * len(conditions)  # Initialize counts to 0 for all conditions

            # Update the count for the corresponding condition index
            if condition in conditions:
                condition_index = conditions.index(condition)
                data[age_group][condition_index] = count

    # Sort age groups and conditions for consistent plotting order
    age_groups = sorted(age_groups)
    conditions2 = sorted(conditions2)

    # Prepare data for plotting
    # data_for_plot = {condition: [data[age_group].get(condition, 0) for age_group in age_groups] for condition in
    #                  conditions}

    print(data)
    connection.close()
    return render_template('home/index.html', segment='index', conditions2 = conditions2,data_for_plot = data,conditions = conditions,genders = genders,dataset = dataset,providers=providers,sumTotalBills=sumTotalBills,countsProviders=countsProviders,bloodTypes=bloodTypes,bloodTypesCounts=bloodTypesCounts, labelsCondition=labelsCondition,countsCondition=countsCondition,counts=counts,countsAge=countsAge,totalPatients=totalPatients)


@blueprint.route('/<template>')
@login_required
def route_template(template):

    try:

        if not template.endswith('.html'):
            template += '.html'

        # Detect the current page
        segment = get_segment(request)

        # Serve the file (if exists) from app/templates/home/FILE.html
        return render_template("home/" + template, segment=segment)

    except TemplateNotFound:
        return render_template('home/page-404.html'), 404

    except:
        return render_template('home/page-500.html'), 500


# Helper - Extract current page name from request
def get_segment(request):

    try:

        segment = request.path.split('/')[-1]

        if segment == '':
            segment = 'index'

        return segment

    except:
        return None
