
from generator101 import *
from datetime import datetime, timedelta
from pytz import utc, timezone
from container import *
import requests

_time_zone = '+8'

#----------------------------------------------------------------------
# Generic tests

def test_on_json():
	print '-------get_json test------'
	ds = DataGenerator()
	df = ds.fetch('000001','20140101','20140201',output='bar')
	jsdoc = df.get_json()
	print type(jsdoc)
	print jsdoc

def test_on_shelve():
	print '-------shelve test------'
	s = Config()
	print s._DATA_WRITE
	print s._DATA_LOADED
	s.override({'name':1})
	print s._DATA_LOADED
	s2 = Config(new=0)
	print s2._DATA_LOADED

def test_on_append_one():
	print '-------append test------'
	b = Bar_dy_Equ1d()
	print b
	b.append_one(['s',0,0,0,0,0])
	b.append_one([1,1,1,1,1,1])
	print b
	b.append_one(['bbb',2,2,2,2,2])
	print b

def test_on_init_client():
	ds = DataGenerator()

def test_on_get1():
	ds = DataGenerator()
	s = requests.session()
	ds.get_all('20150101','20150301','dy_Equity_1d', 30)

def test_on_getall():
	ds = DataGenerator()
	ds.download()

def test_on_fetch():
	ds = DataGenerator()
	ll = ds.fetch('IF1512','20150201', '20150701')
	print ll



if __name__ == '__main__':
	# test_container()
	# test_on_shelve()
	test_on_fetch()


