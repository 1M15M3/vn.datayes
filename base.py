# encoding: UTF-8
import os
import time
import json
import psutil

import tushare as ts
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
# Financial data structure classes.
#----------------------------------------------------------------------
# Tick class

class Tick(pd.DataFrame):
	"""

	"""
	def __init__(self, data=None, index=None):
		super(Tick, self).__init__(data=data, index=index,
			  columns=['time','price','change','volume'])
		self._dtypes = {'time':'object', 'price':'float64',
						'change': 'object', 'volume':'float64'}

		self._buffer = []
		# [{t,price,chg,volume}, ] buffer for real-time appending.

		for col in self.columns:
			self[col] = self[col].astype(self._dtypes[col])
		# Set dtypes.

#----------------------------------------------------------------------
# Bar class

class Bar(pd.DataFrame):
	"""

	"""
	def __init__(self, data=None, index=None):
		super(Bar, self).__init__(data=data, index=index,
			  columns=['time','open','close','high','low','volume'])
		self._dtypes = {'time':'object','open':'float64','close':'float64',
			  		 'high':'float64','low':'float64','volume':'float64'}
		self._ochl_names = ['open','close','high','low']

		self._buffer = []
		# [{t,o,c,h,l,v}, ] buffer for real-time appending.

		self._cand_List = []
		# [(t,o,c,h,l), ] tuples list for matplotlib.finance.ochl_cdstick

		for col in self.columns:
			self[col] = self[col].astype(self._dtypes[col])
		# Set dtypes.

	def get_candlist(self):
		"""
		Initialize [[t,o,c,h,l],] list for mpl.finance.candlestick_ochl.
		"""
		if not self.empty:
			_df = self
			_df['index'] = range(len(_df))
			return _df[['index','open',
						'close','high','low']].as_matrix().tolist()
		else: return None

	def append_bar(self, data):
		"""
		Append one-line bar data.

		Parameters
        ----------
        data: dict or list or tuple.
        	* An One-line new bar data to be appended. 
        	* Should be in t-o-c-h-l-v order. 
        	* Should have [object, float64(*5)] dtype when used to 
        	  construct pd.DataFrame.
		"""
		dtype = type(data)
		if len(data) == 6:
			if dtype is dict:
				append_line = [data['time'], data['open'], data['close'],
							   data['high'], data['low'], data['volume']]
				self.loc[len(self)] = append_line
			elif dtype is list:
				self.loc[len(self)] = data
			elif dtype is tuple:
				self.loc[len(self)] = list(data)
			else:
				raise TypeError('New row must be list/tuple/dict object.')
		else:
			raise ValueError('New row must have lenth of 6')

	def append_to_buffer(self, data):
		dtype = type(data)
		if len(data) == 6:
			if dtype is dict:
				self._buffer.append(data)
			elif dtype is list or dtype is tuple:
				new_dict = {'time':data[0], 'open':data[1], 'close':data[2],
							'high':data[3], 'low':data[4], 'volume':data[5]}
				self._buffer.append(new_dict)
			else:
				raise TypeError('New row must be list/tuple/dict object.')
		else:
			raise ValueError('New row must have lenth of 6')
			
	def concat_bars(self, frame, ignore_index=False):
		_buffer = self._buffer # transfer buffer.
		new_frame = pd.concat([self, frame],
								ignore_index = ignore_index)
		self.__init__(data = new_frame)
		self._buffer = _buffer

	def concat_from_buffer(self, clear=True, ignore_index=False):
		"""
		Concatenate buffer to bar DataFrame.

		Parameters
        ----------
        clear: boolean
        	Whether the buffer is reset after concat.
        ignore_index: boolean
        	Passed to pd.concat(..., ignore_index=), same function.
		"""
		_buffer = self._buffer
		if _buffer:
			if self.empty:
				self.append_bar(_buffer[0])
				_new_frame = pd.concat([self, pd.DataFrame(
										data = _buffer[1:], 
										index = range(1,len(_buffer)))],
										ignore_index = ignore_index)
			else:
				df = pd.DataFrame(_buffer)
				new_frame = pd.concat([self, pd.DataFrame(_buffer)],
										ignore_index = ignore_index)
			self.__init__(data = new_frame)
			if clear:
				self._buffer = []
		else:
			pass

	def distribute_bar(self):
		"""
		Bar Generator, distribute new bar to strategies.
		"""
		if not self.empty:
			t = 0
			while t < len(self):
				yield {'index': t,
					   'data': self.iloc[t]}
				t += 1
		else:
			pass

#----------------------------------------------------------------------
# Bar on date class.

class Bar_1d(pd.DataFrame):
	"""

	"""
	def __init__(self, data=None, index=None):
		super(Bar_1d, self).__init__(data=data, index=index,
			  columns=['date','openPrice','closePrice','highestPrice',
			  		   'lowestPrice','turnoverVol'])
		self._dtypes = {'date':'object',
						'openPrice':'float64',
						'closePrice':'float64',
			  		 	'highestPrice':'float64',
			  		 	'lowestPrice':'float64',
			  		 	'turnoverVol':'float64'}
		self._ochl_names = ['open','close','high','low']

		self._buffer = []
		# [{t,o,c,h,l,v}, ] buffer for real-time appending.

		self._cand_List = []
		# [(t,o,c,h,l), ] tuples list for matplotlib.finance.ochl_cdstick

		for col in self.columns:
			self[col] = self[col].astype(self._dtypes[col])
		# Set dtypes.

		self.columns = ['time','open','close','high','low','volume']

	def get_candlist(self):
		"""
		Initialize [[t,o,c,h,l],] list for mpl.finance.candlestick_ochl.
		"""
		if not self.empty:
			_df = self
			# add index column to temp df.
			_df['index'] = range(len(_df))
			return _df[['index','open',
						'close','high','low']].as_matrix().tolist()
		else: return None


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

	# Setting tushare client.
	_tushare_list_EquSymbols = []
	_PATH_ALLSYMBOLS_EQUD_TS = 'config/tushare_allEqud.json'


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
		"""
		if not os.path.isfile('db_config.json'):
			config = pd.DataFrame({'symbol': list_symbols,
								   'start': [np.nan]*len(list_symbols),
								   'end': [np.nan]*len(list_symbols)})
			config = config.set_index('symbol')
			config.to_json('db_config.json')
			self._db_config = config
		else: 
			self._db_config = pd.read_json('db_config.json')
		"""
			
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
	# tushare get equity methods.
	# Deprecated stuffs...

	def _tushare_init_symbols(self):
		"""
		Initialize all stock symbols supported in tushare APi.
		"""
		"""
		root_dir = 'config'
		if not os.path.exists(root_dir):
			os.makedirs(root_dir)
		PATH_ALLSYMBOLS_EQUD = self._PATH_ALLSYMBOLS_EQUD_TS

		if not os.path.isfile(PATH_ALLSYMBOLS_EQUD):
			df = ts.get_today_all()
			df[['code','name']].to_json(PATH_ALLSYMBOLS_EQUD)
		else:
			df = pd.read_json(PATH_ALLSYMBOLS_EQUD)

		list_symbols = list(df['code'])
		list_symbols = [(6-len(str(s)))*'0'+str(s) for s in list_symbols]
		self._tushare_list_EquSymbols = list_symbols
		"""
		pass

	def _ts_getEquTick(self, req_session, date, ticker=None, save=True):
		"""
		Get tick data on some day and upsert into MongoDB.
		Not exposed to user.

		Parameters
	    ----------
	    * Same as dy_getEqu1d methods (date is only one day).
		"""
		pass

	#----------------------------------------------------------------------
	# MongoDB fetch method.

	def fetch(self, ticker, start, end, field=-1, output='list'):
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
				Fields are:
				{'turnoverRate':..,
				 'turnoverValue':..,
				 'exchangeCD':..,
				 'tradeDate': string date.
				 'date': datetime.datetime date.
				 'turnoverVol':..,
				 'marketValue':..,
				 'secShortName':..,
				 'actPreClosePrice',
				 'openPrice',
				 'lowestPrice',
				 'ticker',
				 'closePrice',
				 'secID',
				 'highestPrice',
				 'negMarketValue',
				 'PE1',
				 'PB',
				 'accumAdjFactor',
				 'PE',
				 'dealAmount',
				 'preClosePrice'
				}
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
			bar = Bar_1d(df[['date','openPrice','closePrice',
					         'highestPrice','lowestPrice','turnoverVol']])
			return bar



#----------------------------------------------------------------------
# Resampler class
#----------------------------------------------------------------------

class Resampler:
	"""
	A Resampler that collects tickdata, 
	and transform them into bar data at any sampling rate.
	"""
	_tick_loaded = False
	_bar_loaded = False
	_ticks = Tick()
	_bars = Bar()

	def __init__(self):
		pass

	def load_ticks(self, data):
		"""
		Load in tick dataframe.

		Parameters
	    ----------
	    data : data.Tick(pd.DataFrame) object.
		"""
		if type(data) is Tick:
			self._ticks = data
			self._tick_loaded = True
		else: 
			raise TypeError('[RS]: Data should be of Tick frame type.')
		if self._ticks.empty:
			print '[RS]: Warning: Empty tick data loaded.'

	def load_bars(self, data):
		"""
		Load in bar dataframe.

		Parameters
	    ----------
	    data : data.Bar(pd.DataFrame) object.
		"""
		if type(data) is Bar or type(data) is Bar_1d:
			self._bars = data
			self._bar_loaded = True
		else:
			raise TypeError('[RS]: Data should be of Bar frame type.')
		if self._bars.empty:
			print '[RS]: Warning: Empty bar data loaded.'

	#----------------------------------------------------------------------
	# Resample methods

	def rspftick(self, rate):
		"""
		Resample loaded tick data at given rate.

		Parameters
	    ----------
	    rate : dict; datetime.timedelta object
	    	This param specifies the sampling rate, i.e.
	    	the time span of ONE bar.
	    	can be either:
	    		* dict.
	    		* timedelta() object.
	    	note that rate should have at most three layers:
	    	day, hour and minute.

	   	Examples
	    --------
	    >>> rs = Resampler()
	    >>> rs.load_ticks(some_tick_data)
	    >>> df = rs.rspftick({'minute':5})
	    >>> df2 = rs.rspftick(timedelta(hours=1, minutes=30))
	    ...
		"""
		valid_keys = ['minute','hour','day']
		if type(rate) is dict:
			for key in rate:
				if key not in valid_keys:
					raise ValueError('[RS]: Illegal key value.')
			if 'minute' in rate:
				_m = rate['minute']
			else: _m = 0
			if 'hour' in rate:
				_h = rate['hour']
			else: _h = 0
			if 'day' in rate:
				_d = rate['day']
			else: _d = 0
		elif type(rate) is timedelta:
			_d = rate.days
			_h, _m = rate.seconds//3600, (rate.seconds//60)%60
		else:
			raise TypeError('[RS]: rate must be dict or timedelta object.')

		if not self._ticks.empty:
			
			# Specify inline functions
			def last(df): return df.iloc[-1]
			todt = lambda str_dt: datetime.strptime(str_dt,
									'%Y-%m-%d %H:%M:%S')
			get_yr = lambda dt: dt.year
			get_m = lambda dt: dt.month

			if _d > 0:
				_d = _d + _h/24.0 + _m/86400.0
				get_d = lambda dt: int(dt.day/_d)
				get_hr = lambda dt: 0
				get_min = lambda dt: 0
			else: 
				if _h > 0:
					_h = _h + _m/60.0
					get_d = lambda dt: dt.day
					get_hr = lambda dt: int(dt.hour/_h)
					get_min = lambda dt: 0
				else:
					get_d = lambda dt: dt.day
					get_hr = lambda dt: dt.hour
					get_min = lambda dt: int(dt.minute/_m)

			f_mapping = {'year':get_yr, 'month': get_m, 
						 'day': get_d, 'hour': get_hr, 'minute': get_min}

			# create temp _df.
			df = self._ticks
			df['datetime'] = df['time'].apply(todt)
			for col in f_mapping:
				df[col] = df['datetime'].apply(f_mapping[col])
			grouped = df.groupby(list(f_mapping), as_index=False)
			grouped = grouped.agg({'price': [np.max, np.min, last],
									 'volume': np.sum,
									 'datetime': last}) # use last
			grouped['index'] = range(len(grouped))
			# calc open.
			grouped[('open','open')] = grouped[('price','last')].shift(1)
			grouped.loc[0,'open'] = df['price'].iloc[0]
			grouped[('high','high')] = grouped[[('price','amax'),
									    ('open','open')]].max(axis=1)
			grouped[('low','low')] = grouped[[('price','amin'),
									    ('open','open')]].min(axis=1)

			data = {'time': grouped[('datetime','last')],
					 'volume': grouped[('volume','sum')],
					 'open': grouped[('open','open')],
					 'close': grouped[('price','last')],
					 'high': grouped[('high','high')],
					 'low': grouped[('low','low')]
			}
			bars = Bar(data)
			bars = bars.sort('time')
			bars = bars.reset_index(drop=True)
			return bars
		else: 
			print '[RS]: Warning: current loaded tick is empty, ' + \
				  'return None.'
			return None

	def rspfbar_date(self, rate):
		"""
		Resample loaded bar data at given rate (days).

		Parameters
	    ----------
	    rate : integer
	    	Specifies the sampling rate, i.e.
	    	the time(date) span of ONE bar. 

	   	Examples
	    --------
	    >>> rs = Resampler()
	    >>> rs.load_bars(some_bar_data)
	    >>> df = rs.rspftick(3) # 3 days.
	    ...
		"""
		if type(rate) != int:
			raise TypeError('[RS]: rate must be an integer.')
		elif rate <= 0:
			raise ValueError('[RS]: Illegal value of rate.')

		if not self._bars.empty:

			# Specify inline functions
			def last(df): return df.iloc[-1]
			chg_d = lambda d: d/rate

			f_mapping = {'day': chg_d}

			# create temp _df.
			df = self._bars
			df['day'] = range(len(df))
			df['day'] = df['day'].apply(f_mapping['day'])
			df['datetime'] = df['time']
			df['price'] = df['close']
				
			grouped = df.groupby(list(f_mapping), as_index=False)
			grouped = grouped.agg({'price': [np.max, np.min, last],
									 'volume': np.sum,
									 'datetime': last}) # use last
			grouped['index'] = range(len(grouped))
			# calc open.
			grouped[('open','open')] = grouped[('price','last')].shift(1)
			grouped.loc[0,'open'] = df['price'].iloc[0]
			grouped[('high','high')] = grouped[[('price','amax'),
									    ('open','open')]].max(axis=1)
			grouped[('low','low')] = grouped[[('price','amin'),
									    ('open','open')]].min(axis=1)

			data = {'time': grouped[('datetime','last')],
					 'volume': grouped[('volume','sum')],
					 'open': grouped[('open','open')],
					 'close': grouped[('price','last')],
					 'high': grouped[('high','high')],
					 'low': grouped[('low','low')]
			}
			bars = Bar(data)
			bars = bars.sort('time')
			bars = bars.reset_index(drop=True)
			return bars
		else: 
			print '[RS]: Warning: current loaded tick is empty, ' + \
				  'return None.'
			return None

#----------------------------------------------------------------------
# Event class.

# Define event types.
EVENT_LOG = 'eLog'
EVENT_TIMER = 'eTimer'
EVENT_NONE = None

class Event:
	"""
	An simplified event message that is pushed into eventEngine
	and distributed to handlers to invoke them.

	Parameters
    ----------
    type: Predefined event type; 
    	  Actually strings that marks different events.
    data: All types of data that is to be handled by listeners.
	"""
	def __init__(self, etype, edata=None):
		self._type = etype
		self._data = edata

#----------------------------------------------------------------------
# Event engine class.

class EventEngine:
	"""

	"""
	# Flag of eventEngine status.
	_active_flag = False

	# About threads.
	_timer = QTimer()
	_queue = Queue()
	_thread_name = 'eventEngine_thread'

	# Handlers mapping.
	_handlers_mapping = dict()

	def __init__(self):
		"""
		Initialize event engine.
		"""
		self._timer.timeout.connect(self._onTimer)
		self._thread = Thread(target = self._distribute, name=_thread_name)
		
	def register(self, etype, func):
		"""
		Register function to a type of events.

		Parameters
    	----------
    	etype: Predefined event type; 
    	func: a function or .method object.
		"""
		try:
			handlers = self._handlers_mapping[etype]
		except KeyError:
			handlers = []
			self._handlers_mapping[etype] = handlers

		# Append function to mapping if not in.
		if func not in handlers:
			handlers.append(func)

	def _onTimer(self):
		"""
		Connect to QTimer signals.
		"""
		event = Event(etype=EVENT_TIMER)
		event._data = {'time': datetime.now()}
		self._queue.put(event)

	def _distribute(self):
		"""
		Distribute events to handlers.
		"""
		while self._active_flag:
			try:
				event = self._queue.get()
				if event._type in self._handlers_mapping:
					[f(event) for f in self._handlers_mapping[event._type]]
			except Empty:
				pass
	
	def start(self):
		self._activeFlag = True
		self._thread.start()
		self._timer.start(1000)

	def suspend(self):
		self._activeFlag = False
		self._timer.stop()
		self._thread.join()

	def put(self, event):
		self._queue.put(event)


		

