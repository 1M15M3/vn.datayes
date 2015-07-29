import sys
# Add the ptdraft folder path to the sys.path list
sys.path.append('/Users/zed/Desktop')

from vnhist2.core import generator, container
from vnhist2.core.container import Config

import json
from flask import Flask, render_template

_config = Config()
app = Flask(__name__)

#----------------------------------------------------------------------
# Views.

@app.route('/')
def index():
	return render_template('index.html')

@app.route('/hist')
def hist():
	return render_template('hist.html')

@app.route('/hist/token')
def hist_token():
	tokens = _config._get_allConfig()
	jsdoc = [{'id':1, 'price':10},{'id':2, 'price':10}]
	return render_template('hist-token.html', tokens=tokens)

if __name__ == '__main__':
	app.run()

