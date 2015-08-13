import os
import json
import pymongo
import pandas as pd

from datetime import datetime, timedelta
from api import Config, PyApi
from api import BaseDataContainer, History, Bar

from errors import (VNPAST_ConfigError, VNPAST_RequestError,
VNPAST_DataConstructorError, VNPAST_DatabaseError)

class DBConfig(Config):
	"""
	Json-like config object.

	Contains all kinds of settings relating to database settings.

	privates
	--------
	Inherited from api.Config, plus:
	* client: pymongo.MongoClient() object, the connection
	  that is to be used for this session.

	"""
	head = 'DB config'

	client = pymongo.MongoClient()
	body = {
		'client': client,
		'dbs': {
			'EQU_M1': {
				'self': client['DATAYES_EQUITY_M1'],
				'index': 'dateTime',
				'collNames': 'secID'
			},
			'EQU_D1': {
				'self': client['DATAYES_EQUITY_D1'],
				'index': 'date',
				'collNames': 'equTicker'
			},
			'FUT_D1': {
				'self': client['DATAYES_FUTURE_D1'],
				'index': 'date',
				'collNames': 'futTicker'
			},
			'OPT_D1': {
				'self': client['DATAYES_OPTION_D1'],
				'index': 'date',
				'collNames': 'optTicker'
			},
			'FUD_D1': {
				'self': client['DATAYES_FUND_D1'],
				'index': 'date',
				'collNames': 'fudTicker'
			},
			'IDX_D1': {
				'self': client['DATAYES_INDEX_D1'],
				'index': 'date',
				'collNames': 'idxTicker'
			}
		},
		'dbNames': ['EQU_M1', 'EQU_D1', 'FUT_D1', 
					'OPT_D1', 'FUD_D1', 'IDX_D1']
	}

	def __init__(self, head=None, token=None, body=None):
		""" 
		Reloaded constructor.
		"""
		super(DBConfig, self).__init__(head, token, body)

	def view(self):
		""" Reloaded Prettify printing method. """
		config_view = {
			'dbConfig_head' : self.head,
			'dbConfig_body' : str(self.body),
		}
		print json.dumps(config_view, 
						 indent=4, 
						 sort_keys=True)

#----------------------------------------------------------------------
# MongoDB Controller class

class MongodController(object):
	"""

	"""
	_config = DBConfig()
	_api = None

	_client = None
	_dbs = None
	_dbNames = []
	_collNames = dict()
	_connected = False

	def __init__(self, config, api):
		"""

		"""
		self._api = api # Set Datayes PyApi.
		if config.body:
			try:
				self._config = config.body
				self._client = config.body['client']
				self._dbs = config.body['dbs']
				self._dbNames = config.body['dbNames']
				self._connected = True
			except KeyError:
				msg = '[MONGOD]: Unable to configure database; ' + \
					  'config file is incomplete.'
				raise VNPAST_ConfigError(msg)
			except Exception,e:
				msg = '[MONGOD]: Unable to configure database; ' + str(e)
				raise VNPAST_ConfigError(msg)

		if self._connected:
			#self.__get_coll_names()
			#self.__ensure_index()
			pass


	#----------------------------------------------------------------------
	# Get collection names methods.

	def __md(dName):
		def _md(get):
			def handle(*args, **kwargs):
				try:
					if os.path.isfile(dName):
						# if directory exists, read from it.
						jsonFile = open(dName,'r')
						data = json.loads(jsonFile.read())
						jsonFile.close()
					else:
						# if not, get data via *get method, 
						# then write to the file.
						data = get(*args, **kwargs)
						jsonFile = open(dName, 'w+')
						jsonFile.write(json.dumps(data))
						jsonFile.close()
					#print data
					return data
				except Exception,e:
					raise e
			return handle
		return _md

	@__md('names/equTicker.json')
	def _allEquTickers(self):
		""""""
		data = self._api.get_equity_D1()
		allEquTickers = list(data.body['ticker'])
		return allEquTickers

	@__md('names/secID.json')
	def _allSecIds(self):
		""""""
		data = self._api.get_equity_D1()
		allTickers = list(data.body['ticker'])
		exchangeCDs = list(data.body['exchangeCD'])
		allSecIds = [allTickers[k]+'.'+exchangeCDs[k] for k in range(
					len(allTickers))]
		return allSecIds

	@__md('names/futTicker.json')
	def _allFutTickers(self):
		""""""
		data = self._api.get_future_D1()
		allFutTickers = list(data.body['ticker'])
		return allFutTickers

	@__md('names/optTicker.json')
	def _allOptTickers(self):
		""""""
		data = self._api.get_option_D1()
		allOptTickers = list(data.body['ticker'])
		return allOptTickers

	@__md('names/fudTicker.json')
	def _allFudTickers(self):
		""""""
		data = self._api.get_fund_D1()
		allFudTickers = list(data.body['ticker'])
		return allFudTickers

	@__md('names/idxTicker.json')
	def _allIdxTickers(self):
		""""""
		data = self._api.get_index_D1()
		allIdxTickers = list(data.body['ticker'])
		return allIdxTickers

	@__md('names/bndTicker.json')
	def _allBndTickers(self):
		""""""
		data = self._api.get_bond_D1()
		allBndTickers = list(data.body['ticker'])
		return allBndTickers

	def _get_coll_names(self):
		"""

		"""
		try:
			if not os.path.exists('names'):
				os.makedirs('names')

			self._collNames['equTicker'] = self._allEquTickers()
			self._collNames['fudTicker'] = self._allFudTickers()
			self._collNames['secID'] = self._allSecIds()
			self._collNames['futTicker'] = self._allFutTickers()
			self._collNames['optTicker'] = self._allOptTickers()
			self._collNames['idxTicker'] = self._allIdxTickers()

			print '[MONGOD]: Collection names gotten.'
			return 1
		except AssertionError: 
			warning = '[MONGOD]: Warning, collection names ' + \
					  'is an empty list.'
			print warning
		except Exception, e:
			msg = '[MONGOD]: Unable to set collection names; ' + \
				   str(e)
			raise VNPAST_DatabaseError(msg)

	#----------------------------------------------------------------------
	# Ensure collection index method.

	def _ensure_index(self):
		"""

		"""
		if self._collNames and self._dbs:
			try:
				for dbName in self._dbs:
					# Iterate over database configurations.

					db = self._dbs[dbName]
					dbSelf = db['self']
					index = db['index']
					collNames = self._collNames[db['collNames']]
					# db['self'] is the pymongo.Database() object.

					for name in collNames:
						coll = dbSelf[name]
						coll.ensure_index([(index, 
											pymongo.DESCENDING)], unique=True)
				print '[MONGOD]: MongoDB index set.'
				return 1
			except KeyError:
				msg = '[MONGOD]: Unable to set collection indices; ' + \
					  'infomation in Config.body["dbs"] is incomplete.'
				raise VNPAST_DatabaseError(msg)
			except Exception, e:
				msg = '[MONGOD]: Unable to set collection indices; ' + str(e)
				raise VNPAST_DatabaseError(msg)

	#----------------------------------------------------------------------
	# Download method.

	def download_equity_D1(self, start, end, sessionNum=30):
		"""

		"""
		try:
			db = self._dbs['EQU_D1']['self']
			self._api.get_equity_D1_mongod(db, start, end, sessionNum)
		except Exception, e:
			msg = '[MONGOD]: Unable to download data; ' + str(e)
			raise VNPAST_DatabaseError(msg)

	def download_equity_M1(self, tasks, startYr=2012, endYr=2015):
		"""

		"""
		try:
			db = self._dbs['EQU_M1']['self']
			self._api.get_equity_M1_interMonth(db, id=1,
								     startYr = startYr,
								     endYr = endYr,
								     tasks = tasks)
		except Exception, e:
			msg = '[MONGOD]: Unable to download data; ' + str(e)
			raise VNPAST_DatabaseError(msg)

	def download_future_D1(self, start, end, sessionNum=30):
		"""

		"""
		pass

	def download_option_D1(self, start, end, sessionNum=30):
		"""

		"""
		pass

	def download_index_D1(self, start, end, sessionNum=30):
		"""

		"""
		pass

	def download_fund_D1(self, start, end, sessionNum=30):
		"""

		"""
		pass

	#----------------------------------------------------------------------
	# Update methods.

	def update_equity_D1(self, sessionNum=30):
		"""

		"""
		try:
			# set databases and tickers
			db = self._dbs['EQU_D1']['self']
			allEquTickers = self._allEquTickers()
			coll = db[allEquTickers[0]]

			# find the latest timestamp in collection.
			latest = coll.find_one(
					 sort=[('date',pymongo.DESCENDING)])['date']
			start = datetime.strftime(latest + timedelta(days=1),'%Y%m%d')
			end = datetime.strftime(datetime.now(), '%Y%m%d')

			# then download.
			self._api.get_equity_D1_mongod(db, start, end, sessionNum)
			
		except Exception, e:
			msg = '[MONGOD]: Unable to update data; ' + str(e)
			raise VNPAST_DatabaseError(msg)

	def update_equity_M1(self, sessionNum=30):
		"""

		"""
		pass

	def update_future_D1(self, sessionNum=30):
		"""

		"""
		pass

	def update_option_D1(self, sessionNum=30):
		"""

		"""
		pass

	def update_index_D1(self, sessionNum=30):
		"""

		"""
		pass

	def update_fund_D1(self, sessionNum=30):
		"""

		"""
		pass

	#----------------------------------------------------------------------
	# Fetch method.

	def fetch(self, dbName, ticker, start, end, output='list'):
		"""

		"""
		# check inputs' validity.
		if output not in ['df', 'list', 'json']:
			raise ValueError('[MONGOD]: Unsupported output type.')
		if dbName not in self._dbNames:
			raise ValueError('[MONGOD]: Unable to locate database name.')

		db = self._dbs[dbName]
		dbSelf = db['self']
		dbIndex = db['index']
		try:
			coll = db[ticker]
			if len(start)==8 and len(end)==8:
				# yyyymmdd, len()=8
				start = datetime.strptime(start, '%Y%m%d')
				end = datetime.strptime(end, '%Y%m%d')
			elif len(start)==14 and len(end)==14:
				# yyyymmdd HH:MM, len()=14
				start = datetime.strptime(start, '%Y%m%d %H:%M')
				end = datetime.strptime(end, '%Y%m%d %H:%M')
			else:
				pass
			docs = []

			# find in MongoDB.
			for doc in coll.find(filter={dbIndex: {'$lte': end,
				'$gte': start}}, projection={'_id': False}):
				docs.append(doc)

			if output == 'list':
				return docs[::-1]

		except Exception, e:
			msg = '[MONGOD]: Error encountered when fetching data' + \
				  'from MongoDB; '+ str(e)
			return -1





