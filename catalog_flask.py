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

from catalog_rebuilder import rebuild_task, getListOfFiles
from catalog_tools import csw_getCount, rejected_delete, csw_truncateRecords, get_xml_file_count


from flask import Flask, render_template, request, url_for, redirect, jsonify
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash
from requests.auth import HTTPBasicAuth as BasicAuth
from solrindexer import IndexMMD

from main import CONFIG

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

    def __init__(self, *args, **kwargs):
        if not CONFIG.readConfig(configFile=os.environ.get("DMCI_CONFIG", None)):
            sys.exit(1)
        super().__init__(__name__)

        self._conf = CONFIG

        if self._conf.distributor_cache is None:
            logger.error("Parameter distributor_cache in config is not set")
            sys.exit(1)

        if self._conf.mmd_xsd_path is None:
            logger.error("Parameter mmd_xsd_path in config is not set")
            sys.exit(1)

        if self._conf.path_to_parent_list is None:
            logger.error("Parameter path_to_parent_list in config is not set")
            sys.exit(1)

        logger.debug(self._conf.call_distributors)

        """ Initialize APP """
        self.auth = HTTPBasicAuth()
        if os.environ.get("TEMPLATE_FOLDER", None) is not None:
            self.template_folder = os.environ.get("TEMPLATE_FOLDER", None)
        else:
            self.template_folder = 'templates'
        logger.debug(self.template_folder)
        logger.debug(os.path.abspath(os.path.dirname(__file__)))

        """Read CSW CONFIG """
        self.csw_username = os.environ.get("PG_CSW_USERNAME", None)
        self.csw_password = os.environ.get("PG_CSW_PASSWORD", None)
        # db_url = "postgis-operator"
        db_url = os.environ.get("PG_CSW_DB_URL", None)
        self.csw_connection = psycopg2.connect(host=db_url,
                                               user=self.csw_username,
                                               password=self.csw_password,
                                               dbname='csw_db', port=5432)
        self.csw_connection.autocommit = True

        """Solr connection"""
        if self._conf.solr_username is not None and self._conf.solr_password is not None:
            auth = BasicAuth(self._conf.solr_username, self._conf.solr_password)
        else:
            auth = None
        self.mysolr = IndexMMD(self._conf.solr_service_url, authentication=auth)

        self.current_task_id = None  # current task id
        self.task = None  # current running task object
        self.prev_task_status = None  # Keep track of previous task

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
            logger.debug(self.template_folder)
            archive_files = _get_archive_files_count()
            rejected_files = _get_rejected_files_count()
            workdir_files = _get_workdir_files_count()
            """ Get CSW records"""
            csw_records = csw_getCount(self.csw_connection)

            solr_docs, solr_current = _get_solr_count()
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
            archive_files = _get_archive_files_count()
            rejected_files = _get_rejected_files_count()
            workdir_files = _get_workdir_files_count()
            """ Get CSW records"""
            csw_records = csw_getCount(self.csw_connection)
            solr_docs, solr_current = _get_solr_count()

            if self.task is not None:
                logger.debug(self.task.state)
                if self.task.state == 'FAILURE' or self.task.state == 'SUCCESS':
                    self.prev_task_status = str(self.task.state) + " : " + str(self.task.info)
                    self.task = None
                    self.current_task_id = None

            return render_template("admin.html",
                                   archive_files=archive_files,
                                   csw_records=csw_records,
                                   solr_docs=solr_docs,
                                   solr_current=solr_current,
                                   rejected_files=rejected_files,
                                   workdir_files=workdir_files,
                                   current_task=self.current_task_id,
                                   prev_task_status=self.prev_task_status,
                                   )

        # POST Clean rejected folder
        @self.route("/rejected/clean", methods=["POST"])
        @self.auth.login_required
        def clean_rejected():
            """ Clean / Delete all files in rejected folder"""
            status, msg = rejected_delete(self._conf.rejected_jobs_path)
            referrer = request.referrer
            if referrer is not None:
                if '/admin' in referrer:
                    return redirect(url_for('admin'))
            if status is True:
                return {"message": "OK"}, 200
            if status is False:
                return {"message": msg}, 500

        # POST Delete all solr documents in index
        @self.route("/solr/clean", methods=["POST"])
        @self.auth.login_required
        def clean_solr():
            try:
                self.mysolr.solrc.delete(q='*:*')
                self.mysolr.commit()
                referrer = request.referrer
                if referrer is not None:
                    if '/admin' in referrer:
                        return redirect(url_for('admin'))

                return {"message": "OK"}, 200
            except Exception as e:
                return {"message": e}, 500

        # POST Truncate pycsw records talbe
        @self.route("/pycsw/clean", methods=["POST"])
        @self.auth.login_required
        def clean_pycsw():
            """ clean / delete pycsw"""
            status, msg = csw_truncateRecords(self.csw_connection)
            referrer = request.referrer
            if referrer is not None:
                if '/admin' in referrer:
                    return redirect(url_for('admin'))

            if status is True:
                return {"message": "OK"}, 200
            if status is False:
                return {"message": msg}, 500

        # POST Rebuild catalog.
        @self.route("/rebuild", methods=["POST"])
        @self.auth.login_required
        def rebuild_catalog():
            """ Rebuld the catalog. given the

            Input parameters:
            dist : list - list of distributors [solr,pycsw]
            """
            logger.debug("call list given in form %s", request.form.getlist('dist'))
            logger.debug(request.form.get('action'))
            distributors = request.form.getlist('dist')
            dmci_action = str(request.form.get('action'))
            self._conf.call_distributors = distributors
            logger.debug("dmci config dist call list: %s", self._conf.call_distributors)
            try:
                task = rebuild_task.apply_async([dmci_action,
                                                 self._conf.path_to_parent_list,
                                                 distributors])
                self.current_task_id = task.id
                self.task = task

                return jsonify({}), 202, {'Location': url_for('rebuild_taskstatus',
                                          task_id=task.id)}
            except Exception as e:
                message = "Somthing went wrong running task. is redis and celery running? "
                return message + e, 500
            # return "OK", 200

        # POST Rebuild catalog.
        @self.route("/rebuild/cancel", methods=["POST"])
        @self.auth.login_required
        def cancel_rebuild():
            response = {'state': 'REVOKED',
                        'current': 1,
                        'total': 1,
                        'status': 'Manually canceled',  # this is the exception raised
                        }
            if self.task is not None:
                self.task.revoke(terminate=True)
                self.prev_task_status = "Revoked" + " : " + "Manually canceled."
                self.task = None
                self.current_task_id = None

            return jsonify(response)

        @self.route('/status/<task_id>')
        def rebuild_taskstatus(task_id):
            try:
                task = rebuild_task.AsyncResult(task_id)
                logger.debug(task.state)
            except Exception as e:
                return jsonify({'status': str(e)})
            if task.state == 'PENDING':
                response = {
                    'state': task.state,
                    'current': 0,
                    'total': 1,
                    'status': task.info.get('status', '')

                }
            elif task.state != 'FAILURE':
                response = {
                    'state': task.state,
                    'current': task.info.get('current', 0),
                    'total': task.info.get('total', 1),
                    'status': task.info.get('status', '')

                }
                if 'result' in task.info:
                    response['result'] = task.info['result']
            else:
                # something went wrong in the background job
                response = {
                    'state': task.state,
                    'current': 1,
                    'total': 1,
                    'status': str(task.info),  # this is the exception raised
                }
                #
            return jsonify(response)

        def _get_archive_files_count():
            """Get Archive files list"""
            archive_files_list = getListOfFiles(
                self._conf.file_archive_path)
            if archive_files_list is None:
                archive_files = 0
            else:
                archive_files = len(archive_files_list)
            return archive_files

        def _get_rejected_files_count():
            """Get Rejected files list"""
            rejected_files = get_xml_file_count(
                self._conf.rejected_jobs_path)
            return rejected_files

        def _get_workdir_files_count():
            """Get Workdir files list"""
            workdir_files = get_xml_file_count(
                self._conf.distributor_cache)
            return workdir_files

        def _get_solr_count():
            """Get SOLR documents"""
            solr_status = self.mysolr.get_status()
            solr_docs = solr_status['numDocs']
            solr_current = solr_status['current']
            return solr_docs, solr_current
