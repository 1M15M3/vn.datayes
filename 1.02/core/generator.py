# encoding: UTF-8
import os
import time

import numpy as np
import pandas as pd

import requests
from requests.exceptions import ConnectionError
from Queue import Queue, Empty
from threading import Thread, Timer
import pymongo
from pymongo import MongoClient
from datetime import datetime, timedelta
from PyQt4.QtCore import QTimer

from container import Bar_dy_Equ1d

#######################################################################
#######################################################################
#######################################################################

# 通联数据用户token

token = '7d434b5530094e2ef8ea242afb28db' + \
		'b19c074a541434f18a07246a540a2574e9'

#######################################################################
#######################################################################
#######################################################################

#----------------------------------------------------------------------
# DataGenerator class
#----------------------------------------------------------------------

class DataGenerator(object):
	"""


	"""
	# Setting datayes client.
	_datayes_host = 'http://api.wmcloud.com/data/v1'
	_datayes_token = token
	_header = {'Connection': 'keep-alive',
			   'Authorization': 'Bearer '+ _datayes_token}
	_datayes_list_EquSymbols = []
	_datayes_list_FutSymbols = []
	_PATH_ALLSYMBOLS_EQUD_DY = 'config/datayes_allEqud.json'
	_PATH_ALLSYMBOLS_FUTD_DY = 'config/datayes_allFutd.json'

	# MongoDB.
	_mongod_connected = False
	_mongod_client = None
	_datayes_dbs = []
	_datayes_db_names = ['datayes_Equ1d', 'datayes_Fut1d']
	_tushare_dbs = []
	_tushare_db_names = ['tushare_tick']
	_db_config = None

	def __init__(self):

		self._init_MongoDB()
		self._init_MongoIndex()

	#----------------------------------------------------------------------
	# Initializing methods.

	def _datayes_init_symbols(self):
		"""
		Initialize all stock/future symbols supported in datayes APi.
		"""
		root_dir = 'config'
		if not os.path.exists(root_dir):
			os.makedirs(root_dir)
		PATH_ALLSYMBOLS_EQUD = self._PATH_ALLSYMBOLS_EQUD_DY
		PATH_ALLSYMBOLS_FUTD = self._PATH_ALLSYMBOLS_FUTD_DY

		yesterday = datetime.strftime(datetime.now()-timedelta(days=1), 
									  "%Y%m%d")
		# Euqity Symbols.
		if not os.path.isfile(PATH_ALLSYMBOLS_EQUD):
			url = self._datayes_host + '/api/market/getMktEqud.json?' + \
			'field=&beginDate=&endDate=&secID=&ticker=&tradeDate=%s' % yesterday
			response = requests.get(url, headers=self._header)
			df_e = pd.DataFrame(response.json()['data'])
			df_e[['ticker','exchangeCD']].to_json(PATH_ALLSYMBOLS_EQUD)
		else:
			df_e = pd.read_json(PATH_ALLSYMBOLS_EQUD)

		# Future Symbols.
		if not os.path.isfile(PATH_ALLSYMBOLS_FUTD):
			url = self._datayes_host + '/api/market/getMktFutd.json?' + \
			'field=&beginDate=&endDate=&secID=&ticker=&tradeDate=%s' % yesterday
			response = requests.get(url, headers=self._header)
			df_f = pd.DataFrame(response.json()['data'])
			df_f[['ticker','exchangeCD']].to_json(PATH_ALLSYMBOLS_FUTD)
		else:
			df_f = pd.read_json(PATH_ALLSYMBOLS_FUTD)

		# Generate list, add zeros.
		list_EquSymbols = list(df_e['ticker'])
		list_EquSymbols = [(6-len(str(s)))*'0'+str(s) for s in list_EquSymbols]

		list_FutSymbols = list(df_f['ticker'])
			
		# set symbols list.
		self._datayes_list_EquSymbols = list_EquSymbols
		self._datayes_list_FutSymbols = list_FutSymbols

	def _init_MongoDB(self):
		"""
		Initialize MongoDB to save datayes data locally.
		"""
		if not self._datayes_list_EquSymbols or \
		   not self._datayes_list_EquSymbols:
			self._datayes_init_symbols()
		if self._mongod_connected == False:
			self._mongod_client = MongoClient()
			self._mongod_connected = True

		names = self._datayes_db_names
		self._datayes_dbs = [self._mongod_client[name] for name in names]

		"""
		names2 = self._tushare_db_names
		self._tushare_dbs = [self._mongod_client[name] for name in names2]
		"""
		print '[DS]: MongoDB initialized.'

	def _init_MongoIndex(self):
		"""
		Initialize MongoDB index on TradeDate.
		"""
		db = self._datayes_dbs[0]
		for ticker in self._datayes_list_EquSymbols:
			coll = db[ticker]
			#coll.drop_indexes()
			coll.ensure_index([('date',pymongo.DESCENDING)], unique=True)

		db = self._datayes_dbs[1]
		for ticker in self._datayes_list_FutSymbols:
			coll = db[ticker]
			#coll.drop_indexes()
			coll.ensure_index([('date',pymongo.DESCENDING)], unique=True)

		print '[DS]: MongoDB index set.'


	#----------------------------------------------------------------------
	# Decorators.

	def _view_Mongo(method):
		"""
		Decorator, view MongoDB.
		Require *method returning a Mongo collection.
		"""
		def handle_func(*args, **kwargs):
			try:
				ret = method(*args, **kwargs) # ret is a Mongo collection.
				print '[MongoDB]: ' + str(ret.count()) + ' docs in collection.'
				print '-------------------------------------------------'
				for doc in ret.find():
					print doc
			except Exception,e:
				raise e
		return handle_func

	#----------------------------------------------------------------------
	# Datayes get equity methods.

	def _dy_getEqu1dBar(self, req_session, start, end, ticker=None, save=True):
		"""
		Use datayes data Api to fetch 1 day bar and upsert into MongoDB.
		for some symbols on some specified time range.
		Not exposed to user.

		Parameters
	    ----------
	    * req_session: requests.session() object.
	    	   Request session used for getting.
	    * start, end: str.
	    	  'yyyymmdd' format date-like string.
	   	* ticker: str.
	   		   Instrument symbol.
		"""

		# str to datetime inline
		todt = lambda str_dt: datetime.strptime(str_dt,'%Y-%m-%d')
		update_dt = lambda d: d.update({'date':todt(d['tradeDate'])})
		url = self._datayes_host + '/api/market/getMktEqud.json?' + \
			'field=&beginDate={}&endDate={}&secID=&'.format(start, end) + \
			'ticker={}&tradeDate='.format(ticker)
		try:
			# Get and clean data.
			response = requests.get(url, headers=self._header)
			data = response.json()['data']
			map(update_dt, data)

			# Connect to MongoDB
			db = self._datayes_dbs[0] # 0 in euqity db index.
			coll = db[ticker]

			coll.insert_many(data)
			return coll # for viewing (decorator).
		except ConnectionError,e:
			# If choke connection, standby for 1sec an invoke again.
			time.sleep(1) 
			self._dy_getEqu1dBar(req_session, start, end, ticker)

	def _dyGetEqu1dBar_wrapper(self, req_session, s_id, tasks, start, end):
		"""
		Wrapper for dy_getEqu1dBar method: assign tasks, print messages.
		not exposed to users, invoked in multithreading get.

		Parameters
	    ----------
	    * req_session: request.session() object.
	    * s_id: Integer.
	    	  Session id.
	   	* tasks: List.
	   		  List of symbols'string; task assigned to session.
	    * start, end: String.
	    	  'yyyymmdd' format date-like string.
	   		  
		"""
		start_time = time.time()
		N = len(tasks)
		k = 0 # counter

		for ticker in tasks:
			self._dy_getEqu1dBar(req_session, start, end, ticker)
			k += 1
			print '[DS|Session{}]: {}/{} tickers finished'.format(s_id,k,N) + \
					' in {} seconds.'.format(int(time.time()-start_time))
		print '[DS|Session{}]: Done.'.format(s_id)

	def getEqu1d(self, start, end, n_session=30):
		"""
		Multithreading get equity 1d-bar method.

		Parameters
	    ----------
	    * start, end: String.
	    	  'yyyymmdd' format date-like string.
	   	* n_session: integer.
	   		   Number of sessions.
		"""
		
		all_tasks = self._datayes_list_EquSymbols
		chunk_size = len(all_tasks)/n_session
		task_lists = [all_tasks[k:k + chunk_size] for k in range(
						0, len(all_tasks), chunk_size)]
		k = 0
		for tasks in task_lists:
			s = requests.session()
			thrd = Thread(target=self._dyGetEqu1dBar_wrapper,
						  args=(s, k, tasks, start, end))
			thrd.start()
			k += 1

	def updateEqu1d(self, n_session=30):
		"""
		Update all collections of 1d-bar data of all equities,
		till totay.
		"""
		all_tasks = self._datayes_list_EquSymbols
		chunk_size = len(all_tasks)/n_session
		task_lists = [all_tasks[k:k + chunk_size] for k in range(
						0, len(all_tasks), chunk_size)]
		db = self._datayes_dbs[0]
		coll = db[all_tasks[0]]
		latest = coll.find_one(sort=[('date',pymongo.DESCENDING)])['date']
		# find existing ending date from database.
		start = datetime.strftime(latest + timedelta(days=1),
										  '%Y%m%d')
		end = datetime.strftime(datetime.now(), '%Y%m%d')
		k = 0
		for tasks in task_lists:
			s = requests.session()
			thrd = Thread(target=self._dyGetEqu1dBar_wrapper,
						  args=(s, k, tasks, start, end))
			thrd.start()
			k += 1

	#----------------------------------------------------------------------
	# Datayes get future methods.

	def _dy_getFut1dBar(self, req_session, start, end, ticker=None, save=True):
		"""
		Get 1 day bar of futures and upsert into MongoDB.
		Not exposed to user.

		Parameters
	    ----------
	    * Same as getEqu1d methods.
		"""
		todt = lambda str_dt: datetime.strptime(str_dt,'%Y-%m-%d')
		update_dt = lambda d: d.update({'date':todt(d['tradeDate'])})
		url = self._datayes_host + '/api/market/getMktFutd.json?' + \
			'field=&beginDate={}&endDate={}&secID=&'.format(start, end) + \
			'ticker={}&tradeDate='.format(ticker)
		try:
			# Get and clean data.
			response = requests.get(url, headers=self._header)
			data = response.json()['data']
			map(update_dt, data)

			# Connect to MongoDB
			db = self._datayes_dbs[1] # 1 is future db index.
			coll = db[ticker]

			coll.insert_many(data)
			return coll # for viewing (decorator).
		except ConnectionError,e:
			# If choke connection, standby for 1sec an invoke again.
			time.sleep(1) 
			self._dy_getFut1dBar(req_session, start, end, ticker)
		except pymongo.errors.BulkWriteError, e:
			print '[DS]: BulkWriteError, some docs in bulk already exist.'
			pass

	def _dyGetFut1dBar_wrapper(self, req_session, s_id, tasks, start, end):
		"""
		Wrapper for dy_getFut1dBar method. Same as Equd_wrapper.
		"""
		start_time = time.time()
		N = len(tasks)
		k = 0 # counter

		for ticker in tasks:
			self._dy_getFut1dBar(req_session, start, end, ticker)
			k += 1
			print '[DS|Session{}]: {}/{} tickers finished'.format(s_id,k,N) + \
					' in {} seconds.'.format(int(time.time()-start_time))
		print '[DS|Session{}]: Done.'.format(s_id)

	def getFut1d(self, start, end, n_session=30):
		"""
		Multithreading get future 1d-bar method.
		"""
		
		all_tasks = self._datayes_list_FutSymbols
		chunk_size = len(all_tasks)/n_session
		task_lists = [all_tasks[k:k + chunk_size] for k in range(
						0, len(all_tasks), chunk_size)]
		k = 0
		for tasks in task_lists:
			s = requests.session()
			thrd = Thread(target=self._dyGetFut1dBar_wrapper,
						  args=(s, k, tasks, start, end))
			thrd.start()
			k += 1

	def updateFut1d(self, n_session=30):
		"""
		Update all collections of 1d-bar data of all futures,
		till totay.
		"""
		all_tasks = self._datayes_list_FutSymbols
		chunk_size = len(all_tasks)/n_session
		task_lists = [all_tasks[k:k + chunk_size] for k in range(
						0, len(all_tasks), chunk_size)]
		db = self._datayes_dbs[1]
		coll = db[all_tasks[0]]
		latest = coll.find_one(sort=[('date',pymongo.DESCENDING)])['date']
		start = datetime.strftime(latest + timedelta(days=1),
										  '%Y%m%d')
		end = datetime.strftime(datetime.now(), '%Y%m%d')
		k = 0
		for tasks in task_lists:
			s = requests.session()
			thrd = Thread(target=self._dyGetFut1dBar_wrapper,
						  args=(s, k, tasks, start, end))
			thrd.start()
			k += 1

	#----------------------------------------------------------------------
	# Datayes get/update all.

	def download(self):
		"""

		"""
		self.getEqu1d('20130101','20150720')
		self.getFut1d('20150101','20150720')

	def update(self):
		"""

		"""
		self.updateFut1d()
		self.updateEqu1d()

	#----------------------------------------------------------------------
	# MongoDB fetch method.

	def fetch(self, ticker, start, end, output='list', field=-1):
		"""
		Load data from MongoDB, return a list of docs

		Parameters
	    ----------
	    ticker: string.
				Equity or Future ticker.
		start, end: string.
				'yyyymmdd' date-like string.
		field: list.
				A list of features that are to be extracted.
				* If not set, return all fields.
		output: string.
				Specifies the output datastructure.
				either 'df' or 'list'(of dicts) or 'bar'.
				* if use 'bar' option, field is treated as default.
		"""
		if output not in ['df','list','bar']:
			raise ValueError('[DS]: Unsupported output type.')
		if ticker in self._datayes_list_EquSymbols:
			db = self._datayes_dbs[0]
		elif ticker in self._datayes_list_FutSymbols:
			db = self._datayes_dbs[1]
		else:
			raise ValueError('[DS]: Symbol not found.')

		coll = db[ticker]
		start = datetime.strptime(start, '%Y%m%d')
		end = datetime.strptime(end, '%Y%m%d')
		docs = []

		if field == -1 or output == 'bar':
			for doc in coll.find(filter={"date": {'$lte': end,
				'$gte': start}}, projection={'_id': False}):
				docs.append(doc)
		elif type(field) is list:
			projection = dict(zip(field,[True]*len(field)))
			projection['_id'] = False
			projection['date'] = True
			for doc in coll.find(filter={"date": {'$lte': end,
				'$gte': start}}, projection=projection):
				docs.append(doc)
		else:
			raise TypeError('[DS]: Field must be a list.')

		if output=='list':
			return docs[::-1]
		elif output == 'df':
			df = pd.DataFrame(docs).sort('date')
			df = df.reset_index(drop=True)
			return df
		elif output == 'bar':			
			df = pd.DataFrame(docs).sort('date')
			df = df.reset_index(drop=True)
			bar = Bar_dy_Equ1d(df[['date','openPrice','closePrice',
					         'highestPrice','lowestPrice','turnoverVol']])
			return bar



