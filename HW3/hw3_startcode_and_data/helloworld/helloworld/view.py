from django.http import HttpResponse
from django.shortcuts import render
import pandas_gbq
from google.oauth2 import service_account

# Make sure you have installed pandas-gbq at first;
# You can use the other way to query BigQuery.
# please have a look at
# https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-nodejs
# To get your credential

credentials = service_account.Credentials.from_service_account_file("")


def hello(request):
    context = {}
    context['content1'] = 'Hello World!'
    return render(request, 'helloworld.html', context)


def dashboard(request):
    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = "big-data-analytics-6893"

    SQL = "select time, ai, data, good, movie, spark \
           from `wordcount.word_count_res` limit 8"
    df = pandas_gbq.read_gbq(SQL)
    # print(df)

    records = df.to_dict(orient='records')
    # print("records:", records)

    data = {}

    l = []

    for record in records:
        d = {}
        d["Time"] = record["time"].strftime(format="%H:%M:%S")
        record.pop("time")
        d["count"] = record
        # print(d)
        l.append(d)

    data["data"] = l

    return render(request, 'dashboard.html', data)


def connection(request):
    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = "big-data-analytics-6893"

    SQL1 = 'select node from `nodes_and_edges.nodes`'
    df1 = pandas_gbq.read_gbq(SQL1)
    # print(df1)

    SQL2 = 'select source, target from `nodes_and_edges.edges`'
    df2 = pandas_gbq.read_gbq(SQL2)
    # print(df2)

    data = {}

    data['n'] = df1.to_dict(orient='records')
    data['e'] = df2.to_dict(orient='records')

    # print(data['n'])
    # print(data['e'])

    return render(request, 'connection.html', data)
