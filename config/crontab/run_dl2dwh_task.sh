#!/bin/bash
cd /home/ocuadmin/Oculus
TASK_ID=$(/home/ocuadmin/Oculus/.venv/bin/celery -A oculus.tasks call oculus.tasks.move_data_to_dwh_task)
echo "$(date '+%Y-%m-%d %H:%M:%S') - Started Task: $TASK_ID" >> /home/ocuadmin/Oculus/logs/move_to_dwh.log
