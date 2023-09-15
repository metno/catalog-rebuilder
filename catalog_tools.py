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
import requests
from lxml import etree

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


def csw_getParentCount(connection):
    cur = connection.cursor()
    cur.execute("""SELECT COUNT(*) FROM RECORDS WHERE TYPE='series'""")
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


def get_solrstatus(solr_url, authentication=None):
    """Get SolR core status information"""
    tmp = solr_url.split('/')
    core = tmp[-1]
    base_url = '/'.join(tmp[0:-1])
    logger.debug("Getting status with url %s and core %s", base_url, core)
    res = None
    try:
        res = requests.get(base_url +
                           '/admin/cores?action=STATUS&core=' + core,
                           auth=authentication)
        res.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        logger.error("Http Error: %s", errh)
    except requests.exceptions.ConnectionError as errc:
        logger.error("Error Connecting: %s", errc)
    except requests.exceptions.Timeout as errt:
        logger.error("Timeout Error: %s", errt)
    except requests.exceptions.RequestException as err:
        logger.error("OOps: Something Else went wrong: %s", err)

    if res is None:
        return None
    else:
        status = res.json()
        return status['status'][core]['index']


def get_solrParentCount(solr_url, authentication=None):
    """Get SolR parent_count"""
    res = None
    try:
        res = requests.get(solr_url +
                           '/select?q=*:*&fq=isParent:true&rows=0',
                           auth=authentication)
        res.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        logger.error("Http Error: %s", errh)
    except requests.exceptions.ConnectionError as errc:
        logger.error("Error Connecting: %s", errc)
    except requests.exceptions.Timeout as errt:
        logger.error("Timeout Error: %s", errt)
    except requests.exceptions.RequestException as err:
        logger.error("OOps: Something Else went wrong: %s", err)

    if res is None:
        return None
    else:
        status = res.json()
        return status['response']['numFound']


def get_unique_parent_refs(solr_url, authentication=None):
    """Get SolR unique parent reference count"""
    res = None
    try:
        res = requests.get(solr_url +
                           '/select?q=*:*&json.facet.x=' +
                           '"unique(related_dataset)"&wt=json&indent=true&rows=0',
                           auth=authentication)
        res.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        logger.error("Http Error: %s", errh)
    except requests.exceptions.ConnectionError as errc:
        logger.error("Error Connecting: %s", errc)
    except requests.exceptions.Timeout as errt:
        logger.error("Timeout Error: %s", errt)
    except requests.exceptions.RequestException as err:
        logger.error("OOps: Something Else went wrong: %s", err)

    if res is None:
        return None
    else:
        status = res.json()
        return status['facets']['x']


def countParentUUIDList(uuid_list):
    doc = etree.parse(uuid_list)
    root = doc.getroot()

    result = len(root.getchildren())
    return result
