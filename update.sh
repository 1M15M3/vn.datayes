#!/bin/bash

python - << EOF

from storage import *
dc = DBConfig()
api = PyApi(Config())
mc = MongodController(dc, api)

mc.update_equity_D1()

EOF