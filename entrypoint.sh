!#/bin/bash
python catalog_rebuilder.py
pkill -QUIT gunicorn
