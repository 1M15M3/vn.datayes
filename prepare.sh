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
dc = DBConfig()
api = PyApi(Config())
mc = MongodController(dc, api)

EOF

echo [vn-past]: Configuration finished.
