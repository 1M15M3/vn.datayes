from flask import Flask, render_template
import pymongo
import json
from generator import *
from container import *


app = Flask(__name__)
ds = DataGenerator()

@app.route('/')
def index():
	cfig = ds._cfig_json
	cfig_str = json.dumps(cfig)
	return render_template('past.html', cfig=cfig_str)
	
if __name__ == "__main__":
	app.run(debug = True)
