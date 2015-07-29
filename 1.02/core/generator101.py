#encoding: UTF-8
#----------------------------------------------------------------------
import os
import time
import requests
import json
import pymongo
import pandas as pd
import numpy as np

from requests.exceptions import ConnectionError
from pymongo import MongoClient
from PyQt4.QtCore import QTimer
from Queue import Queue, Empty
from threading import Thread, Timer
from datetime import datetime, timedelta

from container import Bar_dy_Equ1d, Config




#----------------------------------------------------------------------
# DataGenerator Object.

class DataGenerator(object):
	"""
	The generic DataGenerator class.
	Initialized with a json-config file that looks like:
	{'users':[
		{
			'name': 'zed',
			'host': 'http://api.wmcloud.com/data/v1',
			'header' : {
							'Connection': 'keep-alive',
		   					'Authorization': 'Bearer '
		   			   },
			'token_name': 'zed_token1',
			'token_body': '###########'
			'description': 'Equity_1d and Future_1d'
		}
	'dbs': [
		{
			'name': 'dy_Equity_1d',
			'freq': '1d',
			'index': 'date',
			'tickers': ['000001', ...],
			'size': 900,
			'last_update': '20150715',
			'data_range': ['20120101','20150715'],
			'url_key': 'getEquid'
		},
		{
			'name': 'dy_Future_1d',
			'freq': '1d',
			'index':' date',
			'tickers': ['IF1508', ...],
			'size': 120,
			'last_update': '20150720',
			'data_range': ['20150101','20150720'],
			'url_key': 'getFutd'
		}]}

	The DataGenerator object:
	* Uses client specified by 'users' to:
	* Collects data and store them in databases.
	* Updates itself and override config .json.
	* Provides methods to extract data from dbs.

	"""
	_cfig = None
	_cfig_json = dict()

	# About client.
	_client_configs = dict()

	# About Database.
	_mongod_connected = False
	_mongod_client = None
	_db_configs = dict()
	_dbs = dict()

	def __init__(self):

		self._cfig = Config(new=False)
		self._init_client()
		self._init_mongod()
		self._init_symbols()
		self._init_mongod_index()

	#----------------------------------------------------------------------
	# Decorators.

	def _view(method):
		"""
		Decorator, view returned contents, just for tests.
		"""
		def handle_func(*args, **kwargs):
			try:
				ret = method(*args, **kwargs)
				print ret
			except Exception,e:
				raise e
		return handle_func

	def _view_mongod(method):
		"""
		Decorator, print MongoDB docs.
		Require *method returning a Mongo collection, just for tests.
		"""
		def handle_func(*args, **kwargs):
			try:
				ret = method(*args, **kwargs) 
				# ret is a Mongo collection.
				print '[MongoDB]: ' + str(ret.count()) + \
					  ' docs in collection.'
				print '------------------------------------------'
				for doc in ret.find():
					print doc
			except Exception,e:
				raise e
		return handle_func

	#----------------------------------------------------------------------
	# Initialize

	#@_view
	def _init_client(self):
		""" Initialize client. """
		if not self._cfig:
			self._cfig = Config(new=False)
		self._cfig_json = self._cfig._DATA_LOADED

		# set client configs.
		self._client_configs = self._cfig_json['users'][0]

		print '[DS]: Client configs initialized.'
		return self._client_configs

	#@_view
	def _init_mongod(self):
		""" Initialize databases. """
		if not self._cfig_json:
			self._init_client()

		# set database configs.
		self._db_configs = self._cfig_json['dbs']

		if not self._mongod_connected:
			self._mongod_client = MongoClient()
			self._mongod_connected = True

		cfigs = self._db_configs
		# zip client.db and names together.
		names = [doc['name'] for doc in cfigs]
		dbs = [self._mongod_client[name] for name in names]
		self._dbs = dict(zip(names, dbs))

		print '[DS]: MongoDB initialized.'
		return self._dbs

	#@_view
	def _init_symbols(self):
		""" 
		Initialize symbols, extra code must be added here 
		since the datayes urls are specific.
		"""
		datayes_host = self._client_configs['host']
		header = self._client_configs['header']
		yday = datetime.strftime(datetime.now()-timedelta(days=1), 
									  "%Y%m%d")
		k = 0 
		for db in self._db_configs:
			if not db['tickers']:
				url = datayes_host + '/api/market/%s.json?' % db['url_key'] + \
				'field=&beginDate=&endDate=&secID=&ticker=&tradeDate=%s' % yday
				response = requests.get(url, headers=header)
				df = pd.DataFrame(response.json()['data'])
				list_ticker = list(df['ticker'])
				self._db_configs[k]['tickers'] = list_ticker

				self._cfig_json['dbs'][k]['tickers'] = list_ticker
				self._cfig.override(self._cfig_json)
			else: 
				list_ticker = self._db_configs[k]['tickers']
			k += 1

		return self._cfig_json

	def _init_mongod_index(self):
		""" set mongodb indices. """
		if not self._dbs:
			self._init_mongod()
		k = 0
		for cfigs in self._db_configs:
			name = cfigs['name']
			db = self._dbs[name]
			tickers = cfigs['tickers']
			index_specified = cfigs['index']
			# loop over every collection, ensure index.
			for ticker in tickers:
				coll = db[ticker]
				coll.ensure_index([(index_specified, 
								    pymongo.DESCENDING)], unique=True)
			k += 1
		print '[DS]: MongoDB index set.'

	#----------------------------------------------------------------------
	# Datayes get methods.

	#@_view_mongod
	def get1ticker_byKey(self, req_session, cfig, start, end, ticker=None):
		"""
		Use datayes data Api to fetch /market data(1-day) 
		and upsert into MongoDB;
		for some instrument(specified by url), one symbol, 
		on some specified time range. Not exposed to user.

		Parameters
	    ----------
	    * req_session: requests.session() object.
	    	   Request session used for getting.
	    * cfig: dict.
	    	   json self._db_config[k] object.
	    * start, end: str.
	    	  'yyyymmdd' format date-like string.
	   	* ticker: str.
	   		   Instrument symbol.

		"""
		db_name = cfig['name']
		url_key = cfig['url_key']
		datayes_host = self._client_configs['host']
		header = self._client_configs['header']

		todt = lambda str_dt: datetime.strptime(str_dt,'%Y-%m-%d')
		update_dt = lambda d: d.update({'date':todt(d['tradeDate'])})

		url = datayes_host + '/api/market/%s.json?' % url_key + \
			'field=&beginDate={}&endDate={}&secID=&'.format(start, end) + \
			'ticker={}&tradeDate='.format(ticker)
		try:
			# Get and clean data.
			response = requests.get(url, headers=header)
			data = response.json()['data']
			map(update_dt, data)

			# Connect to MongoDB
			db = self._dbs[db_name]
			coll = db[ticker]

			coll.insert_many(data)
			return coll # for viewing (decorator).
		except ConnectionError,e:
			# If choke connection, standby for 1sec an invoke again.
			time.sleep(1)
			self._get1ticker_byKey(req_session, cfig, start, end, ticker)

	def _get1ticker_wrapper(self, req_session, s_id, cfig, tasks, start, end):
		"""
		Wrapper for get1ticker_byKey method: assign tasks, print messages.
		not exposed to users, invoked in multithreading get.

		Parameters
	    ----------
	    * req_session: request.session() object.
	    * s_id: Integer.
	    	  Session id.
	    * cfig: dict.
	    	  Json self._db_config[k] object.
	   	* tasks: List.
	   		  List of symbols'string; task assigned to session.
	    * start, end: String.
	    	  'yyyymmdd' format date-like string.
	   		  
		"""
		start_time = time.time()
		N = len(tasks)
		k = 0 # counter

		for ticker in tasks:
			self.get1ticker_byKey(req_session, cfig, start, end, ticker)
			k += 1
			print '[DS|Session{}]: {}/{} tickers finished'.format(s_id,k,N) + \
					' in {} seconds.'.format(int(time.time()-start_time))
		print '[DS|Session{}]: Done.'.format(s_id)






