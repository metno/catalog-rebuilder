#!/bin/bash
/usr/bin/python3 catalog_rebuilder.py
pkill -QUIT gunicorn
