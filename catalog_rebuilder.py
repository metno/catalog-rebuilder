"""
Catalog rebuilder - script
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

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(name)s:%(asctime)s:%(levelname)s:%(message)s')

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


# Function for concerrntly process list of inputs using multithreading
def concurrently(fn, inputs, *, max_concurrency=20):
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
        parentList.append(p.text)

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
            logger.error('Clould not read file %s error was %s' % (filename, e))
            return None
        return xmlfile.encode()


def check_integrety(num_files, dmci_status_endpoint):
    """
    Function to check that both pycsw and solr have the
    equal amount of records as the input files read from
    the mmd repository archive.

    Will use the dmci status endpoint for testing this.

    A sys.exit(1) with an error should be sendt if not match
    A sys.exit(0) with an success log message if match
    """
    raise NotImplementedError


def dmci_ingest(dmci_url, mmd):
    """
    Given url + endpoint for dmci instance,
    insert the given file.
    """
    url = dmci_url + '/v1/insert'
    try:
        response = requests.post(url, data=mmd)
    except ConnectionError as e:
        logger.error("Could not connect to DMCI rebuilder endpoint %s. Reason: %s" % (url, e))
        sys.exit(1)
    except Exception as e:
        logger.error("An error occured when ingesting to DMCI rebuilder  %s. Reason: %s",
                     (url, e))
        sys.exit(1)
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
            logger.error("Could not ingest parent mmd file %s. Reason: %s" % (parent, msg))
        fileList.remove(parent)

    """Then we ingest all the rest"""
    for (file, mmd) in concurrently(fn=loadFile, inputs=fileList):

        # Get the processed document and its status
        logger.debug("Processing file: %s", file)
        status, msg = dmci_ingest(dmci_url, mmd)
        if status != 200:
            logger.error("Could not ingest mmd file %s. Reason: %s" % (file, msg))

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
    logger.info('Execution time: %s', time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))
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
        logger.error("Missing parents-uuid-list.xml from path %s", parent_uuid_list)
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
