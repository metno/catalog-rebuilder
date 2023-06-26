"""
Catalog rebuilder - tools
====================

Copyright 2021 MET Norway

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(name)s:%(asctime)s:%(levelname)s:%(message)s')

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


def csw_getCount(connection):
    cur = connection.cursor()
    cur.execute("""SELECT COUNT(*) FROM RECORDS""")
    result = cur.fetchall()
    cur.close()
    return result.pop()[0]


def csw_truncateRecords(connection):
    try:
        cur = connection.cursor()
        cur.execute("""TRUNCATE TABLE RECORDS""")
        cur.close()
        # result = cur.fetchall()
        return True, "OK"
    except Exception as e:
        return False, e


def rejected_delete(folder):
    try:
        [f.unlink() for f in Path(folder).glob("*") if f.is_file()]
    except Exception as e:
        logger.error("Failed to delete files: %s", e)
        return False, e
    return True, "OK"


def get_xml_file_count(folder):
    filelist = list(Path(folder).glob('*.xml'))
    if filelist is None:
        return 0
    else:
        return len(filelist)
