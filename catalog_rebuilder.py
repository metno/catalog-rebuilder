"""
Catalog rebuilder - Flask app
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
from concurrent.futures import ThreadPoolExecutor
from concurrent import futures as Futures
import itertools
import requests
import os
import fnmatch
import sys
import time
from lxml import etree
from pathlib import Path
from time import sleep
import logging
from celery import Celery
from itertools import islice
from celery import group
import dmci
from dmci.api.worker import Worker as DmciWorker
from dmci.api.app import App

from dmci.distributors import SolRDist, PyCSWDist

import dmci.distributors.distributor
from solrindexer.indexdata import IndexMMD
from requests.auth import HTTPBasicAuth

from main import CRConfig

"""Bootstrapping Catalog-Rebuilder"""

"""Read the DMCI config object"""
os.curdir = os.path.abspath(os.path.dirname(__file__))
CONFIG = CRConfig()
if not CONFIG.readConfig(configFile=os.environ.get("DMCI_CONFIG", None)):
    sys.exit(1)

"""Overrid DMCI package config"""
dmci.CONFIG = CONFIG  # Not sure if this works


"""Initialize logging"""
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(fmt='[{asctime:}] {name:>28}:{lineno:<4d} {levelname:8s} {message:}',
                              style="{")

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
logging.getLogger('solrindexer').setLevel(logging.WARNING)
logging.getLogger('dmci').setLevel(logging.DEBUG)
# logging.getLogger('pysolr').setLevel(logging.INFO)

"""Initialize Solr Connection object"""
authentication = None
if CONFIG.solr_username is not None and CONFIG.solr_password is not None:
    authentication = HTTPBasicAuth(CONFIG.solr_username,
                                   CONFIG.solr_password)
indexMMD = IndexMMD(CONFIG.solr_service_url, always_commit=False,
                    authentication=authentication)


class CRPyCSWMDist(PyCSWDist):
    """Override PyCSwDist  with the given config read from rebuilder"""
    def __init__(self, cmd, xml_file=None, metadata_id=None, worker=None, **kwargs):
        super().__init__(cmd, xml_file, metadata_id, worker, **kwargs)
        self._conf = CONFIG
        return


class CRSolrDist(SolRDist):
    """Override SolRDist  with the given config read from rebuilder"""
    def __init__(self, cmd, xml_file=None, metadata_id=None, worker=None, **kwargs):
        super().__init__(cmd, xml_file, metadata_id, worker, **kwargs)
        self._conf = CONFIG
        # self._conf.fail_on_missing_parent = False
        self.authentication = self._init_authentication()

        #  Use the initiilized solr connection
        self.mysolr = indexMMD
        self.mysolr.solr_url = self._conf.solr_service_url
        logger.debug(self._conf.solr_service_url)
        return


class Worker(DmciWorker):
    """Ovverride the DMCI Worker with the inherited distributors"""
    CALL_MAP = {
        "pycsw": CRPyCSWMDist,
        "solr": CRSolrDist
    }

    def __init__(self, cmd, xml_file, xsd_validator, dist_call, **kwargs):
        super().__init__(cmd, xml_file, xsd_validator, **kwargs)
        self._conf = CONFIG
        self._conf.call_distributors = dist_call  # Use given dist call list from flask
        dmci.CONFIG = CONFIG
        logger.debug("command:  %s", self._dist_cmd)
        logger.debug("dists:  %s", self._conf.call_distributors)
        logger.debug("xsd:  %s", self._xsd_obj)
        logger.debug("csw url:  %s", self._conf.csw_service_url)
        logger.debug("solr url:  %s", self._conf.solr_service_url)

        return


"""Initialize Celery"""
redis_broker = str(CONFIG.redis_broker) + '/0'
app = Celery('rebuilder',
             broker=redis_broker)
app.conf.update(broker_url=CONFIG.redis_broker,
                result_backend=CONFIG.redis_broker,
                task_serializer='json',
                accept_content=['json'],  # Ignore other content
                result_serializer='json',
                timezone='Europe/Oslo',
                enable_utc=True,
                broker_connection_retry_on_startup=True
                )


"""Initialize global xsd_obj used by dmci distributors"""
XSD_OBJ = None
try:
    XSD_OBJ = etree.XMLSchema(
        etree.parse(CONFIG.mmd_xsd_path))
except Exception as e:
    logger.critical("XML Schema could not be parsed: %s" %
                    str(CONFIG.mmd_xsd_path))
    logger.critical(str(e))
    sys.exit(1)

"""Hack for container warning"""
githack = 'git config --global --add safe.directory '
os.system(githack+CONFIG.mmd_repo_path)


@app.task(bind=True)
def rebuild_task(self, action, parentlist_path, call_distributors):
    """Main Celery Catalog-rebuilder task"""
    logger.info("Requested task %s", self.request.id)
    logger.debug("Call distributors: %s", call_distributors)
    logger.debug("parent list path %s", parentlist_path)
    dmci.config = CONFIG

    self.update_state(state='PENDING',
                      meta={'current': 0, 'total': 1,
                            'status': 'Cloning MMD repo, and reading filenames.'})
    cloneRepo()
    # index_archive = os.environ.get("INDEX_ARCHIVE", None)
    # if index_archive is not None:
    #    INDEX_ARCHIVE = index_archive
    fileList = getListOfFiles(CONFIG.mmd_repo_path)

    """Keep track of ingest tasks and status"""
    total = len(fileList)
    current = 0
    # failed = 0

    self.update_state(state='PROGRESS',
                      meta={'current': current, 'total': total, 'status': 'Preparing parents'})

    parentList = getParentUUIDs(parentlist_path)

    # Keep track of time taken for job.
    st = time.perf_counter()
    pst = time.process_time()

    if fileList is None:
        logger.error("No MMD files found in archive_path: %s", archive_path)
        return {'status': 'No files found in archive path.'}

    """Extract the parent mmd files from the list."""
    parent_mmds = [s for s in fileList if any(xs in s for xs in parentList)]
    logger.info("Found %d parent datasets", len(parent_mmds))
    logger.info("Files to process: %s ", len(fileList))

    # Wait a bit to be sure the dmci-catalog-rebuilder is up and running
    logger.info("Sleeping for one minute to make sure sidecar is running.")

    logger.info("Starting catalog rebuilding....")

    """First we ingest the parents."""
    parentJob = group(dmci_dist_ingest_task.s(file, action,
                                              call_distributors)
                      for file in parent_mmds)()
    self.parentJob = parentJob
    pcount = 0
    while parentJob.waiting():
        pcount = parentJob.completed_count()
        self.update_state(state='PROGRESS',
                          meta={'current': parentJob.completed_count(), 'total': total,
                                'status': 'Processing parents'})

    current += pcount

    """Update fileList remove ingested parents"""
    for parent in parent_mmds:
        fileList.remove(parent)

    self.update_state(state='PROGRESS',
                      meta={'current': current, 'total': total,
                            'status': 'Processing MMD files'})

    """Then we ingest all other datasets"""
    mmdJob = group(dmci_dist_ingest_task.s(file, action, call_distributors)
                   for file in fileList)()
    self.mmdJob = mmdJob
    while mmdJob.waiting():
        current = mmdJob.completed_count() + pcount
        self.update_state(state='PROGRESS',
                          meta={'current': current, 'total': total,
                                'status': 'Processing MMD files'})

    current = mmdJob.completed_count() + pcount

    # End time taking.
    et = time.perf_counter()
    pet = time.process_time()
    elapsed_time = et - st
    pelt = pet - pst
    logger.info('Execution time: %s', time.strftime(
        "%H:%M:%S", time.gmtime(elapsed_time)))
    logger.info('CPU time: %s', time.strftime("%H:%M:%S", time.gmtime(pelt)))
    job_time = time.strftime("%H:%M:%S", time.gmtime(pelt))
    self.update_state(state='SUCCESS',
                      meta={'current': current, 'total': total,
                            'status': 'Catalog rebuilding completed in {0}'.format(job_time)}
                      )

    return {'status': 'Catalog rebuilding completed in {0}'.format(job_time),
            'current': current, 'total': total}


def processFile(file):
    """Process one mmd file, using the DMCI worker"""
    return NotImplementedError


def cloneRepo():
    """ Updates the MMD_REPO. """
    mmd_repo = CONFIG.mmd_repo_url
    destination_path = CONFIG.mmd_repo_path
    if not os.path.exists(destination_path):
        clone_command = "git clone " + mmd_repo + " " + destination_path
    else:
        pushd_command = "cd " + destination_path + '; '
        clone_command = pushd_command + "git pull origin master"
        logger.debug(clone_command)
    os.system(clone_command)


def concurrently(fn, inputs, *, max_concurrency=5):
    """
    Calls the function ``fn`` on the values ``inputs``.
    ``fn`` should be a function that takes a single input, which is the
    individual values in the iterable ``inputs``.
    Generates (input, output) tuples as the calls to ``fn`` complete.
    See https://alexwlchan.net/2019/10/adventures-with-concurrent-futures/ for an explanation
    of how this function works.
    """
    # Make sure we get a consistent iterator throughout, rather than
    # getting the first element repeatedly.
    fn_inputs = iter(inputs)

    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(fn, input): input
            for input in itertools.islice(fn_inputs, max_concurrency)
        }

        while futures:
            done, _ = Futures.wait(
                futures, return_when=Futures.FIRST_COMPLETED, timeout=None
            )

            for fut in done:
                original_input = futures.pop(fut)
                yield original_input, fut.result()

            for input in itertools.islice(fn_inputs, len(done)):
                fut = executor.submit(fn, input)
                futures[fut] = input


def getListOfFiles(dirName):
    """
    create a list of file and sub directories
    names in the given directory
    """
    listOfFiles = list()
    for (dirpath, dirnames, filenames) in os.walk(dirName):
        for filename in fnmatch.filter(filenames, '*.xml'):
            listOfFiles.append(os.path.join(dirpath, filename))

    if len(listOfFiles) == 0:
        return None
    return listOfFiles


def chunks(it, n):
    """Slice an array into chunks and return chunk"""
    for first in it:
        yield [first] + list(islice(it, n - 1))


def getParentUUIDs(xmlfile):
    """Function that reads the parent-uuid-list.xml file and
    return a list of parent UUIDs"""

    try:
        parent_list = etree.parse(xmlfile)
    except Exception as e:
        logger.error("Could not parse the parent-uuid-list.xml, Reason: %s", e)
        sys.exit(1)

    _parentList = parent_list.findall('id')
    parentList = []
    for p in _parentList:
        pid = str(p.text)
        parentList.append(pid.split(':')[1])

    return parentList


def loadFile(filename):
    """
    Load mmd xml file and return file
    """
    filename = filename.strip()
    try:
        file = Path(filename)
    except Exception as e:
        logger.warning('Not a valid filepath %s error was %s' % (filename, e))
        return None
    with open(file, encoding='UTF-8') as fd:
        try:
            xmlfile = fd.read()
        except Exception as e:
            logger.error('Clould not read file %s error was %s' %
                         (filename, e))
            return None
        return xmlfile.encode()


@app.task()
def dmci_dist_ingest_task(mmd_path, action, call_distributors):
    """Celery task ingesting one mmd file"""
    data = loadFile(mmd_path)
    status = False
    worker = Worker(action, mmd_path, XSD_OBJ, call_distributors,
                    path_to_parent_list=CONFIG.path_to_parent_list,
                    md_namespace=CONFIG.env_string)
    valid, msg, data_ = worker.validate(data)
    if not data == data_:
        msg, code = App._persist_file(data_, mmd_path)
    if valid is True:
        status = True
        valid = True
        called = []
        failed = []
        skipped = []
        failed_msg = []
        failed_dict = dict()
        ok_dict = dict()

        for dist in call_distributors:
            if dist not in worker.CALL_MAP:
                continue
            obj = worker.CALL_MAP[dist](
                worker._dist_cmd,
                xml_file=mmd_path,
                metadata_id=worker._dist_metadata_id,
                worker=worker,
                path_to_parent_list=CONFIG.path_to_parent_list
            )
            obj._conf = CONFIG
            valid &= obj.is_valid()
            if obj.is_valid():
                obj._conf = CONFIG
                obj_status, obj_msg = obj.run()
                status &= obj_status
                if obj_status:
                    called.append(dist)
                    ok_dict[dist] = obj_msg
                else:
                    failed.append(dist)
                    failed_msg.append(msg)
                    failed_dict[dist] = obj_msg
            else:
                skipped.append(dist)
        if len(failed) > 0:
            # msg = '\n'.join([msg for msg in failed_msg])
            msg = failed_dict
            status = False
        else:
            status = True
            msg = '\n'.join([msg for msg in called])

    else:
        logger.error("XML Validation failed for file: %s . Reason: %s", mmd_path, msg)

    # logger.debug("Woreker result: %s", failed_dict)

    return (status, mmd_path, msg)


def dmci_dist_ingest(data, mmd_path, action, call_distributors, xsd_obj):
    """Using the distributors directly ingesting"""
    status = False
    worker = Worker(action, mmd_path, xsd_obj, call_distributors,
                    path_to_parent_list=CONFIG.path_to_parent_list,
                    md_namespace=CONFIG.env_string)
    valid, msg, data_ = worker.validate(data)
    if not data == data_:
        msg, code = App._persist_file(data_, mmd_path)
    if valid is True:
        status = True
        valid = True
        called = []
        failed = []
        skipped = []
        failed_msg = []

        for dist in call_distributors:
            if dist not in worker.CALL_MAP:
                skipped.append(dist)
                continue
            obj = worker.CALL_MAP[dist](
                worker._dist_cmd,
                xml_file=mmd_path,
                metadata_id=worker._dist_metadata_id,
                worker=worker,
                path_to_parent_list=CONFIG.path_to_parent_list
            )
            obj._conf = CONFIG
            valid &= obj.is_valid()
            if obj.is_valid():
                obj._conf = CONFIG
                obj_status, obj_msg = obj.run()
                status &= obj_status
                if obj_status:
                    called.append(dist)
                else:
                    failed.append(dist)
                    failed_msg.append(obj_msg)
            else:
                skipped.append(dist)
        if len(failed) > 0:
            msg = mmd_path + ': ' + '\n'.join([msg for msg in failed_msg])
            status = False
        else:
            status = True
            msg = "OK"

    else:
        logger.error("File %s failed validation. Reason: %s", mmd_path, msg)

    return (status, msg)


def dmci_ingest(dmci_url, mmd, action):
    """
    Given url + endpoint for dmci instance,
    insert the given file.
    """
    if action == 'insert':
        url = dmci_url + '/v1/insert'

    elif action == 'update':
        url = dmci_url + '/v1/update'

    else:
        url = dmci_url + '/v1/insert'

    try:
        response = requests.post(url, data=mmd)

    except ConnectionError as e:
        logger.error(
            "Could not connect to DMCI rebuilder endpoint %s. Reason: %s", url, e)

    except Exception as e:
        logger.error("An error occured when ingesting to DMCI rebuilder  %s. Reason: %s",
                     url, e)

    return response.status_code, response.text


def main(archive_path, dmci_url, parent_uuid_list):
    """
    Main function. Get a list of all mmd files in archive and ingest them into custom
    dmci with only csw distributor (solr to be added when ready).
    """

    if not os.path.exists(archive_path):
        logger.error("Could not read from archive path %s" % archive_path)
        sys.exit(1)
    logger.info("Reading from archive path %s" % archive_path)

    dmci_url = os.getenv('DMCI_REBUILDER_URL')
    logger.info("DMCI rebuilder url is %s" % dmci_url)

    parentList = getParentUUIDs(parent_uuid_list)

    # Keep track of time taken for job.
    st = time.perf_counter()
    pst = time.process_time()

    fileList = getListOfFiles(archive_path)
    if fileList is None:
        logger.error("No MMD files found in archive_path: %s", archive_path)
        sys.exit(1)

    """Extract the parent mmd files from the list."""
    parent_mmds = [s for s in fileList if any(xs in s for xs in parentList)]
    logger.info("Found %d parent datasets", len(parent_mmds))
    logger.info("Files to process: %s ", len(fileList))

    # Wait a bit to be sure the dmci-catalog-rebuilder is up and running
    logger.info("Sleeping for one minute to make sure sidecar is running.")
    sleep(60)
    logger.info("Starting catalog rebuilding....")

    """First we ingest the parents."""
    for parent in parent_mmds:
        parent_mmd = loadFile(parent)
        logger.debug("Processing parent file: %s", parent)
        status, msg = dmci_ingest(dmci_url, parent_mmd)
        if status != 200:
            logger.error(
                "Could not ingest parent mmd file %s. Reason: %s" % (parent, msg))
        fileList.remove(parent)

    """Then we ingest all the rest"""
    for (file, mmd) in concurrently(fn=loadFile, inputs=fileList):

        # Get the processed document and its status
        logger.debug("Processing file: %s", file)
        status, msg = dmci_ingest(dmci_url, mmd)
        if status != 200:
            logger.error("Could not ingest mmd file %s. Reason: %s" %
                         (file, msg))

    """
    TODO: Add check here after ingestion is finished to check
    if we have the same number of records as input files.
    """
    # num_files = len(fileList)
    # pycsw_url = os.getenv('PYCSW_URL')
    # solr_url = os.getenv('SOLR_URL')
    # check_integrety(num_files,pycsw_url,solr_url)

    # End time taking.
    et = time.perf_counter()
    pet = time.process_time()
    elapsed_time = et - st
    pelt = pet - pst
    logger.info('Execution time: %s', time.strftime(
        "%H:%M:%S", time.gmtime(elapsed_time)))
    logger.info('CPU time: %s', time.strftime("%H:%M:%S", time.gmtime(pelt)))

    sys.exit(0)


if __name__ == "__main__":
    enabled = os.getenv('CATALOG_REBUILDER_ENABLED')
    archive_path = os.getenv('MMD_ARCHIVE_PATH')
    dmci_url = os.getenv('DMCI_REBUILDER_URL')

    if os.getenv('PARENT_UUID_LIST'):
        parent_uuid_list = os.getenv('PARENT_UUID_LIST')
    else:
        parent_uuid_list = '/parent-uuid-list.xml'

    if not os.path.exists(parent_uuid_list):
        logger.error("Missing parents-uuid-list.xml from path %s",
                     parent_uuid_list)
        sys.exit(1)
    if os.path.exists(parent_uuid_list):
        logger.info("Found parent-uuid-list.xml in %s", parent_uuid_list)
    if os.getenv('DEBUG') is not None:
        logger.info("Setting loglevel to DEBUG")
        logger.setLevel(logging.DEBUG)
        stream_handler.setLevel(logging.DEBUG)
    else:
        logger.info("Log level is INFO")
        logger.setLevel(logging.INFO)
        stream_handler.setLevel(logging.INFO)
    if archive_path is None:
        logger.error("Missing environment variable MMD_ARCHIVE_PATH")
        sys.exit(1)
    if dmci_url is None:
        logger.error("Missing environment variable DMCI_REBUILDER_URL")
        sys.exit(1)
    if enabled == 'True' or enabled == 'true':
        logger.info("Catalog rebuilder enabled. --starting job-- ")
        main(archive_path, dmci_url, parent_uuid_list)
    else:
        logger.info("Catalog rebuilder disabled. --skipping job-- ")
        sleep(60)
    sys.exit(0)
