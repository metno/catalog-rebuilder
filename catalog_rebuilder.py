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
from pathlib import Path
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(name)s:%(asctime)s:%(levelname)s:%(message)s')

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


# Function for concerrntly process list of inputs using multithreading
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
        listOfFiles += [os.path.join(dirpath, file) for file in filenames]


    return listOfFiles


def load_file(filename):
    """
    Load mmd xml file and return file
    """
    filename = filename.strip()
    try:
        file = Path(filename)
    except Exception as e:
        logger.warning('Not a valid filepath %s error was %s' %(filename,e))
        return None
    with open(file, encoding='utf-8') as fd:
        try:
            xmlfile = fd.read()
        except Exception as e:
            logger.error('Clould not read file %s error was %s' %(filename,e))
            return None
        return xmlfile


def dmci_ingest(dmci_url, mmd):
    """
    Given url + endpoint for dmci instance,
    insert the given file.
    """
    url = dmci_url + '/v1/insert'
    response = requests.post(url, data=mmd)
    return response.status_code, response.text



############## PSEUDO CODE ##########################
# - read/create list of all xml files in the archive
# - for each file, send to dmci/insert only using the pycsw distributor
# - when job is finished the number of records in archive and csw-catalog should match
# - In kubernetes this should either be a pod that stops when finished, or using kind: Job annotation.

archive_path = os.environ.get('ARCHIVE_PATH') # Defined in deployment.yaml
dmci_url = os.environ.get('DMCI_URL') # Defined in deployment.yaml
fileList = getListOfFiles(archive_path)

for(file, mmd) in concurrently(fn=load_file, inputs=fileList):

            # Get the processed document and its status
            status, msg = dmci_ingest(dmci_url,mmd)
            if status != 200:
                 logger.error("Could not ingest mmd file %s. Reason: %s" %(file,msg))