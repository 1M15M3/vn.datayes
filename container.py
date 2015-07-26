from datetime import datetime, timedelta
import pandas as pd
import json

#----------------------------------------------------------------------
# Container classes

class Bar_dy_Equ1d(pd.DataFrame):
	"""
	Bar container to collect datayes interday equity data.
	Loaded time format is datetime(y,m,d,0,0).
	"""
	# Initialize as highchart friendly or matplotlib
	# friendly form.
	# *It seems like matplotlib.finance added ohlc order.

	_time_zone = '+8' # Time zone adjustment for Unix timestamp.
	_init_as = 'highcharts'

	# column names of imports and outputs.
	_column_namesf = ['date','openPrice','highestPrice',
			  		  'lowestPrice','closePrice','turnoverVol']
	_column_namest = ['time','open','high','low','close','volume']
	_ohlc_names = ['open','high','low','close']
	_list_names = ['time','open','high','low','close','volume']

	# column datatypes.
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
		Decorator, view MongoDB.
		Require *method returning a Mongo collection.
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
		Get list format.
		[[time, open, high, low, close, volume], ]

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
