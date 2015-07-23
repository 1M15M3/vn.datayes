import pandas as pd

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