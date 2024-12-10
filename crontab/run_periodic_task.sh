#!/bin/bash
cd /home/ocuadmin/Oculus
TASK_ID=$(/home/ocuadmin/Oculus/.venv/bin/celery -A oculus.tasks call oculus.tasks.periodic_fetch_task --args='[24]')
echo "$(date '+%Y-%m-%d %H:%M:%S') - Started Task: $TASK_ID" >> /home/ocuadmin/Oculus/logs/fetch_daily.log
