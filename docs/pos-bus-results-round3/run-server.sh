#!/usr/bin/env bash
set -euo pipefail
export MALLOC_ARENA_MAX=2
export POS_BUS_PROBE_DIR=/home/coder/playground/pos-bus-investigation/round3/probe
export ODOO_REGISTRY_LRU_SIZE=4
export ODOO_REGISTRY_LRU_SIZE_CRON=1
exec /home/coder/venvs/odoo/master/bin/python /home/coder/worktrees/justpickles00/odoo/master/odoo-bin -c /home/coder/playground/pos-bus-investigation/odoo.conf --load=base,web,bus,pos_bus_probe
