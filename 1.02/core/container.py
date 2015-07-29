from datetime import datetime, timedelta
import pandas as pd
import json
import os


#----------------------------------------------------------------------
# Config container Object.

class Config(object):

	_CPATH = 'config.json'
	_SH = None

	# JSON formatted config infomation to be written.
	_DATA_WRITE = {
		'users':[
			{
				'name': 'zed',
				'host': 'http://api.wmcloud.com/data/v1',
				'header' : {
								'Connection': 'keep-alive',
			   					'Authorization': 'Bearer ' + \
			   					'7d434b5530094e2ef8ea242afb28db' + \
								'b19c074a541434f18a07246a540a2574e9'
			   			   },
				'token_name': 'zed_token1',
				'token_body': '7d434b5530094e2ef8ea242afb28db' + \
								'b19c074a541434f18a07246a540a2574e9',
				'description': 'Equity_1d and Future_1d'
			}
		],
		'dbs': [
			{
				'name': 'dy_Equity_1d',
				'freq': '1d',
				'index': 'date',
				'tickers': [],
				'size': 0,
				'last_update': None,
				'data_range': None,
				'url_key': 'getMktEqud'
			},
			{
				'name': 'dy_Future_1d',
				'freq': '1d',
				'index': 'date',
				'tickers': [],
				'size': 0,
				'last_update': None,
				'data_range': None,
				'url_key': 'getMktFutd'
			}
		]
	}

	# JSON formatted config infomation loaded in. (Dictionary)
	_DATA_LOADED = dict()

	def __init__(self, new=1):
		self._init_config_file(new=new)

	#----------------------------------------------------------------------
	# Decorator
	def _view(method):
		"""
		Decorator, view returned contents, just for tests.
		"""
		def handle_list(*args, **kwargs):
			try:
				ret = method(*args, **kwargs)
				print ret
				return ret
			except Exception,e:
				raise e
		return handle_list

	#@_view
	def _init_config_file(self, new):
		"""
		Initialize config file.

		Parameters
        ----------
        new: boolean
        	* True: Force to write a new config file(even there exists one) 
        	  or use existing file.
        	* False: Use existing config.json, write if not exists.
		"""
		if new or (not new and not os.path.isfile(self._CPATH)):
			# First write.
			jsfile = open(self._CPATH, 'w+')
			jsfile.write(json.dumps(self._DATA_WRITE))
			jsfile.close()

			# Then load in.
			jsfile = open(self._CPATH,'r')
			doc = json.loads(jsfile.read())

		elif not new and os.path.isfile(self._CPATH):
			# Load in.
			jsfile = open(self._CPATH,'r')
			doc = json.loads(jsfile.read())

		self._DATA_LOADED = doc
		return doc

	def override(self, new_json):
		"""
		Override the config.json by new dict.

		Parameters
        ----------
        jsdoc: dict(json-like)
		"""
		self._DATA_WRITE = new_json
		self._init_config_file(new=True)

	#@_view
	def _get_allConfig(self):
		"""
		Return config info as json string.
		"""
		return json.dumps(self._DATA_LOADED['users'])

	def get_usrs(self):
		return self._DATA_LOADED['users']

	def get_dbs(self):
		return self._DATA_LOADED['dbs']


#----------------------------------------------------------------------
# Bar Container classes

class Bar_dy_Equ1d(pd.DataFrame):
	"""
	A Generic Bar container to collect datayes interday equity data.
	Loaded time format is datetime(y,m,d,0,0).

	Constructed as a subclass of pd.DataFrame, with preset column names,
	column data types and tranformation settings.
	"""
	# Initialize as highchart-friendly or matplotlib-friendly form.
	# *It seems like matplotlib.finance added ohlc order.
	_init_as = 'highcharts'

	# Time zone adjustment for Unix timestamp conversion.
	_time_zone = '+8' 
	
	# Default column names of imports and outputs.
	_column_namesf = ['date','openPrice','highestPrice',
			  		  'lowestPrice','closePrice','turnoverVol']
	_column_namest = ['time','open','high','low','close','volume']
	_ohlc_names = ['open','high','low','close']
	_list_names = ['time','open','high','low','close','volume']

	# Default column datatypes.
	_column_dtypes = {'date':'object',
						'openPrice':'float64',
			  		 	'highestPrice':'float64',
			  		 	'lowestPrice':'float64',
			  		 	'closePrice':'float64',
			  		 	'turnoverVol':'float64'}
	# other containers.
	_buffer = []
	_cand_List = []

	def __init__(self, data=None, index=None):

		if self._init_as == 'highcharts':
			super(Bar_dy_Equ1d, self).__init__(data=data, index=index,
			  		 columns=self._column_namesf)
		else: raise ValueError('_init_as value error.') 
		# Just for future extensions.
		self._clean_columns()

	def _clean_columns(self):
		"""
		Set names and datetypes of columns.
		"""
		for col in self.columns:
			self[col] = self[col].astype(self._column_dtypes[col])
		self.columns = self._column_namest

	#----------------------------------------------------------------------
	# Type conversion methods	

	# Decorator
	def _to_json(method):
		"""
		Decorator, dumps a dict-like pythonic variable to javascript-
		readable parameter.
		"""
		def handle_list(*args, **kwargs):
			try:
				ret = method(*args, **kwargs) # ret is a list.
				# convert to json.
				ret = json.dumps(ret)
				return ret
			except Exception,e:
				raise e
		return handle_list

	def get_list(self, time='timestamp'):
		"""
		Metamorphose into list format.
		[[time, open, high, low, close, volume], [],  ]

		Parameters:
		----------
		time: string.
			The format that time is to be presented.
			* 'timestamp': Unix timestamp, in milliseconds.
			* 'datetime': Datetime format.
			* 'index': Integer (starts from zero).
		"""
		if not self.empty:
			df = self
			if type(time) != str:
				raise TypeError('[Bar]: Time format must be string.')
			if time == 'timestamp':
				to_timestamp = lambda dt: int(dt.strftime("%s"))*1000 + \
									  	  int(self._time_zone)*3600000
				df['time'] = df['time'].apply(to_timestamp)
			elif time == 'datetime':
				pass
			elif time == 'index':
				df['time'] = range(len(df))
			else:
				raise ValueError('[Bar]: Unsupported time format.')
			list_ = df[self._list_names].as_matrix().tolist()
			return list_

	@_to_json
	def get_json(self, time='timestamp'):
		"""
		Get json format for javascript(highcharts) use.
		Required format is:
		[[timestamp.numeric.ms, open, high, 
		  low, close, volume], [...], ...]
		
		Parameters:
		----------
		time: string. * same as get_list method.
		"""
		return self.get_list(time=time)

	def get_candlist(self):
		"""
		Get specified list for matplotlib.finance.candlestick().
		"""
		return self.get_candlist(time='index')

	#----------------------------------------------------------------------
	# DataFrame manipulation methods.

	def append_one(self, data):
		"""
		Append one-line bar data.

		Parameters
        ----------
        data: dict or list or tuple.
        	* An One-line new bar data to be appended. 
        	* Should be in t-o-h-l-c-v order. 
        	* Should have [object, float64(*5)] dtype when used to 
        	  construct pd.DataFrame.
		"""
		dtype = type(data)
		if len(data) == 6:
			if dtype is dict:
				append_line = [data[self._column_namest[k]] for k in range(6)]
				self.loc[len(self)] = append_line
			elif dtype is list:
				self.loc[len(self)] = data
			elif dtype is tuple:
				self.loc[len(self)] = list(data)
			else:
				raise TypeError('New row must be list/tuple/dict object.')
		else:
			raise ValueError('New row must have lenth of 6')




