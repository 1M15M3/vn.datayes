#!/bin/bash

dir_n=names
if [ ! -d $dir_n ]; then
	mkdir $dir_n
fi

dir_c=config
if [ ! -d $dir_c ]; then
	mkdir $dir_c
fi

echo [vn-past]: Configuration starts.

python - << EOF

from storage import *
import pandas as pd
import os
dc = DBConfig()
api = PyApi(Config())
mc = MongodController(dc, api)

mc._collNames['equTicker'] = mc._allEquTickers()
print '[MONGOD]: Equity tickers collected.'

mc._collNames['secID'] = mc._allSecIds()
print '[MONGOD]: Security IDs collected.'

mc._collNames['futTicker'] = mc._allFutTickers()
print '[MONGOD]: Future tickers collected.'

mc._collNames['optTicker'] = mc._allOptTickers()
print '[MONGOD]: Option symbols collected.'

mc._collNames['fudTicker'] = mc._allFudTickers()
print '[MONGOD]: Mutual Fund symbols collected.'

mc._collNames['idxTicker'] = mc._allIdxTickers()
print '[MONGOD]: Index symbols collected.'

mc._ensure_index()

dbName = 'EQU_D1'
collNames =  mc._dbs['EQU_D1']['self'].collection_names()
colls = [mc._dbs[dbName]['self'][name] for name in collNames]
counts = [coll.count() for coll in colls]

view = pd.DataFrame({'collections':collNames,
					 'counts': counts})

print '[MONGOD]: View collections.tail() in {}.'.format(dbName)
print view.tail()

if view['counts'][0] >= 1:
	isOldDb = 1
else:
	isOldDb = 0

view.to_csv('./config/db_EQU_D1.csv')

EOF

export IFS=","
cat ./config/db_EQU_D1.csv | head -n 2 | tail -n 1 | while read ind ticker count; do : ; done

echo [vn-past]: Configuration finished.
echo [vn-past]: Current collection names: 
cd ./names
ls -l

if [[ $count > 0 ]]
then
	echo [API]: Unintialized database detected.
	echo [API]: Prepare to download Bars {20130101, 20150801}...

	read -r -p "[API]: Confirm? [y/N] " response
	if [[ $response =~ ^([yY][eE][sS]|[yY])$ ]]
	then
		cd -
		chmod +x download.sh
		./download.sh

	else
	    echo [vn-past]: Do not download.
	    :
	fi

else
	echo [API]: Database initialized.
	echo [API]: Prepare to update Bars till latest trading date...

	read -r -p "[API]: Confirm? [y/N] " response
	if [[ $response =~ ^([yY][eE][sS]|[yY])$ ]]
	then
		cd -
		chmod +x update.sh
		./update.sh

	else
	    echo [vn-past]: Do not update.
	    :
	fi
fi
echo [vn-past]: Finished.