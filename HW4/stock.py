import yfinance as yf
import pandas as pd
import json

from datetime import date, datetime, timedelta
import time
import fcntl

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from sklearn import linear_model

day = timedelta(days=1)
today = date.today()

aapl  = yf.Ticker("AAPL")
fb    = yf.Ticker("FB")
googl = yf.Ticker("GOOGL")
msft  = yf.Ticker("MSFT")
amzn  = yf.Ticker("AMZN")


default_args = {
    'owner': 'yy3089',
    'depends_on_past': False,
    'email': ['yy3089@columbia.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

def errorAAPL():
	tdy_df = aapl.history(start=today-2*day)
	tdy_list = tdy_df.values.tolist()
	if len(tdy_list) != 0:
		tdy_high = tdy_list[-1][1]

		with open('./pred.json', 'r') as fp:
			pred = json.load(fp)

		error_aapl = abs(pred['AAPL']-tdy_high)/tdy_high

		return error_aapl

def errorFB():
	tdy_df = fb.history(start=today-2*day)
	tdy_list = tdy_df.values.tolist()
	if len(tdy_list) != 0:
		tdy_high = tdy_list[-1][1]

		with open('./pred.json', 'r') as fp:
			pred_fb = json.load(fp)

		error_fb = abs(pred_fb['FB']-tdy_high)/tdy_high

		return error_fb

def errorGOOGL():
	tdy_df = googl.history(start=today-2*day)
	tdy_list = tdy_df.values.tolist()
	if len(tdy_list) != 0:
		tdy_high = tdy_list[-1][1]

		with open('./pred.json', 'r') as fp:
			pred_googl = json.load(fp)

		error_googl = abs(pred_googl['GOOGL']-tdy_high)/tdy_high

		return error_googl

def errorMSFT():
	tdy_df = msft.history(start=today-2*day)
	tdy_list = tdy_df.values.tolist()
	if len(tdy_list) != 0:
		tdy_high = tdy_list[-1][1]

		with open('./pred.json', 'r') as fp:
			pred_msft = json.load(fp)

		error_msft = abs(pred_msft['MSFT']-tdy_high)/tdy_high

		return error_msft

def errorAMZN():
	tdy_df = amzn.history(sstart=today-2*day)
	tdy_list = tdy_df.values.tolist()
	if len(tdy_list) != 0:
		tdy_high = tdy_list[-1][1]

		with open('./pred.json', 'r') as fp:
			pred = json.load(fp)

		error_amzn = abs(pred['AMZN']-tdy_high)/tdy_high

		return error_amzn

def storeError(**kwargs):
	error_aapl  = kwargs['task_instance'].xcom_pull(task_ids='t1_0')
	error_fb    = kwargs['task_instance'].xcom_pull(task_ids='t2_0')
	error_googl = kwargs['task_instance'].xcom_pull(task_ids='t3_0')
	error_msft  = kwargs['task_instance'].xcom_pull(task_ids='t4_0')
	error_amzn  = kwargs['task_instance'].xcom_pull(task_ids='t5_0')

	error = {"AAPL": error_aapl,
			"FB": error_fb,
			"GOOGL": error_googl,
			"MSFT": error_msft,
			"AMZN": error_amzn}

	with open("./error.json", 'w') as fp:
		json.dump(error, fp, indent=4)


def getAAPL():
	hist_df = aapl.history(period='max')
	hist_df.to_csv('./aapl.csv', sep=',', header=True, index=True)
	AAPLX, AAPLY = [], []
	hist_list = hist_df.values.tolist()
	for i in range(len(hist_list)-1):
		AAPLX.append(hist_list[i][0:5])
		AAPLY.append(hist_list[i+1][1])
	AAPLX.append(hist_list[-1][0:5])

	pred_aapl = fitStock(AAPLX, AAPLY)
	return pred_aapl

def getFB():
	hist_df = fb.history(period='max')
	hist_df.to_csv('./fb.csv', sep=',', header=True, index=True)
	FBX, FBY = [], []
	hist_list = hist_df.values.tolist()
	for i in range(len(hist_list)-1):
		FBX.append(hist_list[i][0:5])
		FBY.append(hist_list[i+1][1])
	FBX.append(hist_list[-1][0:5])

	pred_fb = fitStock(FBX, FBY)
	return pred_fb

def getGOOGL():
	hist_df = googl.history(period='max')
	hist_df.to_csv('./googl.csv', sep=',', header=True, index=True)
	GOOGLX, GOOGLY = [], []
	hist_list = hist_df.values.tolist()
	for i in range(len(hist_list)-1):
		GOOGLX.append(hist_list[i][0:5])
		GOOGLY.append(hist_list[i+1][1])
	GOOGLX.append(hist_list[-1][0:5])

	pred_googl = fitStock(GOOGLX, GOOGLY)
	return pred_googl

def getMSFT():
	hist_df = msft.history(period='max')
	hist_df.to_csv('./msft.csv', sep=',', header=True, index=True)
	MSFTX, MSFTY = [], []
	hist_list = hist_df.values.tolist()
	for i in range(len(hist_list)-1):
		MSFTX.append(hist_list[i][0:5])
		MSFTY.append(hist_list[i+1][1])
	MSFTX.append(hist_list[-1][0:5])

	pred_msft = fitStock(MSFTX, MSFTY)
	return pred_msft

def getAMZN():
	hist_df = amzn.history(period='max')
	hist_df.to_csv('./amzn.csv', sep=',', header=True, index=True)
	AMZNX, AMZNY = [], []
	hist_list = hist_df.values.tolist()
	for i in range(len(hist_list)-1):
		AMZNX.append(hist_list[i][0:5])
		AMZNY.append(hist_list[i+1][1])
	AMZNX.append(hist_list[-1][0:5])

	pred_amzn = fitStock(AMZNX, AMZNY)
	return pred_amzn

def fitStock(X, Y):
	lm = linear_model.LinearRegression()
	lm.fit(X[:-1], Y)
	pred = lm.predict([X[-1][0:5]])[0]

	return pred


def storeResult(**kwargs):
	pred_aapl  = kwargs['task_instance'].xcom_pull(task_ids='t1_1')
	pred_fb    = kwargs['task_instance'].xcom_pull(task_ids='t2_1')
	pred_googl = kwargs['task_instance'].xcom_pull(task_ids='t3_1')
	pred_msft  = kwargs['task_instance'].xcom_pull(task_ids='t4_1')
	pred_amzn  = kwargs['task_instance'].xcom_pull(task_ids='t5_1')

	pred = {"AAPL": pred_aapl,
			"FB": pred_fb,
			"GOOGL": pred_googl,
			"MSFT": pred_msft,
			"AMZN": pred_amzn}

	with open("./pred.json", 'w') as fp:
		json.dump(pred, fp, indent=4)


with DAG(
    'stock',
    default_args=default_args,
    description='Get stock data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 11, 29, 12, 00, 00),
    catchup=False,
    tags=['example'],
) as dag:

	t0_0 = BashOperator(
        task_id='t0_0',
        bash_command='echo Start fetching stock information',
        retries=3,
    )

	t1_0 = PythonOperator(
        task_id='t1_0',
        python_callable=errorAAPL,
        retries=3,
    )

	t2_0 = PythonOperator(
        task_id='t2_0',
        python_callable=errorFB,
        retries=3,
    )

	t3_0 = PythonOperator(
        task_id='t3_0',
        python_callable=errorGOOGL,
        retries=3,
    )

	t4_0 = PythonOperator(
        task_id='t4_0',
        python_callable=errorMSFT,
        retries=3,
    )

	t5_0 = PythonOperator(
        task_id='t5_0',
        python_callable=errorAMZN,
        retries=3,
    )

	t0_1 = PythonOperator(
        task_id='t0_1',
        python_callable=storeError,
        retries=3,
    )

	t1_1 = PythonOperator(
        task_id='t1_1',
        python_callable=getAAPL,
        retries=3,
    )

	t2_1 = PythonOperator(
        task_id='t2_1',
        python_callable=getFB,
        retries=3,
    )

	t3_1 = PythonOperator(
        task_id='t3_1',
        python_callable=getGOOGL,
        retries=3,
    )

	t4_1 = PythonOperator(
        task_id='t4_1',
        python_callable=getMSFT,
        retries=3,
    )

	t5_1 = PythonOperator(
        task_id='t5_1',
        python_callable=getAMZN,
        retries=3,
    )

	t0_2 = PythonOperator(
        task_id='t0_2',
        python_callable=storeResult,
        retries=3,
    )

	t0_0 >> [t1_0, t2_0, t3_0, t4_0, t5_0]
	t1_0 >> t0_1
	t2_0 >> t0_1
	t3_0 >> t0_1
	t4_0 >> t0_1
	t5_0 >> t0_1

	t0_1 >> [t1_1, t2_1, t3_1, t4_1, t5_1]

	t1_1 >> t0_2
	t2_1 >> t0_2
	t3_1 >> t0_2
	t4_1 >> t0_2
	t5_1 >> t0_2