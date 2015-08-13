#!/bin/bash

python - << EOF

from storage import *
dc = DBConfig()
api = PyApi(Config())
mc = MongodController(dc, api)

mc.download_equity_D1('20130101','20150801')

EOF