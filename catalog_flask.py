"""
Catalog rebuilder admin interface - flask app
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

import os
import sys
import logging
import psycopg2

import catalog_rebuilder as cb
from catalog_tools import csw_getCount, rejected_delete, csw_truncateRecords

from lxml import etree

from flask import Flask, render_template, request, flash, send_from_directory, json, Response
from werkzeug.utils import secure_filename
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash

from requests.auth import HTTPBasicAuth as BasicAuth

from filebrowser.funcs import get_size, diff, folderCompare

from dmci.config import Config
from solrindexer import IndexMMD

from browsepy import app as browsepy, plugin_manager

CONFIG = Config()

users = {
    "admin": generate_password_hash("test"),
}

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter(fmt='[{asctime:}] {name:>28}:{lineno:<4d} {levelname:8s} {message:}',
                              style="{")

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


class AdminApp(Flask):

    def __init__(self):
        super().__init__(__name__)

        """ Initialize DMCI Config"""
        CONFIG.readConfig(configFile=os.environ.get("DMCI_CONFIG", None))
        self._conf = CONFIG
        self.auth = HTTPBasicAuth()

        if self._conf.distributor_cache is None:
            logger.error("Parameter distributor_cache in config is not set")
            sys.exit(1)

        if self._conf.mmd_xsd_path is None:
            logger.error("Parameter mmd_xsd_path in config is not set")
            sys.exit(1)

        if self._conf.path_to_parent_list is None:
            logger.error("Parameter path_to_parent_list in config is not set")
            sys.exit(1)

        # Create the XML Validator Object
        logger.debug("Parsing xsd %s", self._conf.mmd_xsd_path)
        try:
            self._xsd_obj = etree.XMLSchema(
                etree.parse(str(self._conf.mmd_xsd_path).strip()))
        except Exception as e:
            logger.critical("XML Schema could not be parsed: %s" %
                            str(self._conf.mmd_xsd_path))
            logger.critical(str(e))
            # sys.exit(1)

        """Read CSW CONFIG """
        self.csw_username = os.environ.get("PG_CSW_USERNAME", None)
        self.csw_password = os.environ.get("PG_CSW_PASSWORD", None)
        # db_url = "postgis-operator"
        db_url = os.environ.get("PG_CSW_DB_URL", None)
        self.csw_connection = psycopg2.connect(host=db_url,
                                               user=self.csw_username,
                                               password=self.csw_password,
                                               dbname='csw_db', port=5432)

        """Solr connection"""
        if self._conf.solr_username is not None and self._conf.solr_password is not None:
            auth = BasicAuth(self._conf.solr_username, self._conf.solr_password)
        else:
            auth = None
        self.solrc = IndexMMD(self._conf.solr_service_url, authentication=auth)

        """initialize browsepy"""
        browsepy.config.update(
            APPLICATION_ROOT='/browse',
            directory_base=self._conf.rejected_jobs_path,
            directory_start=self._conf.rejected_jobs_path,
            directory_remove=self._conf.rejected_jobs_path,
        )
        plugin_manager.reload()

        # Very password

        @self.auth.verify_password
        def verify_password(username, password):
            if username in users and \
                    check_password_hash(users.get(username), password):
                return username

        # Show catalog status
        @self.route("/status")
        def status():
            """ Return integrity status of the catalog. Include number of rejected files"""

            """ Get Archive files list"""
            archive_files_list = cb.getListOfFiles(
                self._conf.file_archive_path)
            if archive_files_list is None:
                archive_files = 0
            else:
                archive_files = len(archive_files_list)

            """ Get Rejected files list"""
            rejected_files_list = cb.getListOfFiles(
                self._conf.rejected_jobs_path)
            if rejected_files_list is None:
                rejected_files = 0
            else:
                rejected_files = len(rejected_files_list)

            """ Get Workdir files list"""
            workdir_files_list = cb.getListOfFiles(
                self._conf.distributor_cache)
            if workdir_files_list is None:
                workdir_files = 0
            else:
                workdir_files = len(workdir_files_list)

            """ Get CSW records"""
            csw_records = csw_getCount(self.csw_connection)

            """ Get SOLR documents"""
            solr_status = self.solrc.get_status()
            solr_docs = solr_status['numDocs']
            solr_current = solr_status['current']
            return render_template("status.html",
                                   archive_files=archive_files,
                                   csw_records=csw_records,
                                   solr_docs=solr_docs,
                                   solr_current=solr_current,
                                   rejected_files=rejected_files,
                                   workdir_files=workdir_files

                                   )

        # Show catalog status
        @self.route("/admin")
        @self.auth.login_required
        def admin():
            """Simple form with buttons to manage rejected files and rebuild catalog.
            Form will have buttons that execute the below POST endpoints"""
            return render_template("admin.html")

        # List Rejected
        @self.route("/rejected")
        def list_rejected():
            """ List files in rejected direcotry. use maybe browsepy or similar
            """

            return NotImplementedError

        # GET All rejected as a zip file
        @self.route("/rejected/get")
        def get_rejected():
            """"Create a zip file of all contents in rejected folder and return"""
            return NotImplementedError

        # POST Clean rejected folder
        @self.route("/rejected/clean", methods=["POST"])
        @self.auth.login_required
        def clean_rejected():
            """ Clean / Delete all files in rejected folder"""
            status, msg = rejected_delete(self._conf.rejected_jobs_path)
            if status is True:
                return {"message": "OK"}, 200
            if status is False:
                return {"message": msg}, 500

        # POST Clean solr index
        @self.route("/solr/clean", methods=["POST"])
        @self.auth.login_required
        def clean_solr():
            """ clean / delete solr index"""
            return NotImplementedError

        # POST Clean solr index
        @self.route("/pycsw/clean", methods=["POST"])
        @self.auth.login_required
        def clean_pycsw():
            """ clean / delete pycsw"""
            status, msg = csw_truncateRecords(self.csw_connection)
            if status is True:
                return {"message": "OK"}, 200
            if status is False:
                return {"message": msg}, 500

        # POST Rebuild catalog.
        @self.route("/rebild", methods=["POST"])
        @self.auth.login_required
        def rebuild_catalog():
            """ Rebuld the catalog. given the

            Input parameters:
            dist : list - list of distributors [solr,pycsw]
            """
            return NotImplementedError

        """From filebrowser"""
        @self.route("/load-data", methods=['POST'])
        def loaddata():
            data = request.get_json()
            name = data['name']
            folder = data['folder']
            curr_path = os.path.join(
                self._conf.rejected_jobs_path, folder, name)
            folders = []
            folders_date = []
            files = []
            files_size = []
            files_date = []
            if folderCompare(self._conf.rejected_jobs_path, curr_path):
                dir_list = os.listdir(curr_path)
                for item in dir_list:
                    if os.path.isdir(os.path.join(curr_path, item)):
                        folders.append(item)
                        folder_date = diff(os.path.join(curr_path, item))
                        folders_date.append(folder_date)
                for item in dir_list:
                    if os.path.isfile(os.path.join(curr_path, item)):
                        files.append(item)
                        file_size = get_size(os.path.join(curr_path, item))
                        files_size.append(file_size)
                        file_date = diff(os.path.join(curr_path, item))
                        files_date.append(file_date)

                folders_data = list(zip(folders, folders_date))
                files_data = list(zip(files, files_size, files_date))

                return render_template('data.html', folders_data=folders_data, files_data=files_data)
            else:
                return '0', 201

        @self.route('/info')
        def info():
            foldername = os.path.basename(self._conf.rejected_jobs_path)
            lastmd = diff(self._conf.rejected_jobs_path)
            dir_list = os.listdir(self._conf.rejected_jobs_path)
            file = 0
            folder = 0
            for item in dir_list:
                if os.path.isdir(item):
                    folder += 1
                elif os.path.isfile(item):
                    file += 1
            data = {'foldername': foldername, 'lastmd': lastmd,
                    'file': file, 'folder': folder}
            return custom_response(data, 200)

        def custom_response(res, status_code):
            return Response(mimetype="application/json",
                            response=json.dumps(res), status=status_code)

        @self.route('/folderlist', methods=['GET'])
        def folderlist():
            curr_path = self._conf.rejected_jobs_path
            logger.debug("current path:  %s", curr_path)
            folders = [curr_path]
            return {"item": folders}
