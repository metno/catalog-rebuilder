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
from time import sleep
from threading import Thread
from datetime import datetime

from catalog_rebuilder import rebuild_task, getListOfFiles, app, dmci_dist_ingest_task
from catalog_tools import csw_getCount, rejected_delete, get_solrstatus
from catalog_tools import csw_truncateRecords, get_xml_file_count


from flask import Flask, render_template, request, url_for, redirect, jsonify
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash
from requests.auth import HTTPBasicAuth as BasicAuth
from solrindexer import IndexMMD

from celery.result import GroupResult, ResultBase
from main import CONFIG, jobdata

"""Initialize logging"""
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
        self.csw_username = os.environ.get("PG_CSW_USERNAME", CONFIG.csw_postgis_user)
        self.csw_password = os.environ.get("PG_CSW_PASSWORD", CONFIG.csw_postgis_password)
        # db_url = "postgis-operator"
        db_url = os.environ.get("PG_CSW_DB_URL", CONFIG.csw_postgis_host)
        logger.info("Connectiong to pycsw postgis: %s", db_url)
        self.csw_connection = psycopg2.connect(host=db_url,
                                               user=self.csw_username,
                                               password=self.csw_password,
                                               dbname='csw_db', port=5432)
        self.csw_connection.autocommit = True
        self.solr_auth = None
        """Solr connection"""
        if self._conf.solr_username is not None and self._conf.solr_password is not None:
            self.solr_auth = BasicAuth(self._conf.solr_username, self._conf.solr_password)
        logger.info("Connecting to SolR: %s", self._conf.solr_service_url)
        self.mysolr = IndexMMD(self._conf.solr_service_url, authentication=self.solr_auth)

        """Flask admin credentials"""
        """TODO: Read from config/kubernets. Create new secrets or reuse solr secrets?"""
        # logger.debug(self._conf.solr_password)
        self.users = {
            "admin": generate_password_hash(self._conf.solr_password),
        }

        # Very password
        @self.auth.verify_password
        def verify_password(username, password):
            if username in self.users and \
                    check_password_hash(self.users.get(username), password):
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

            solr_docs, solr_current = _get_solr_count(self.mysolr.solr_url,
                                                      self.solr_auth)
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
            """Global job result dict"""

            """Get DMCI info"""
            archive_files = _get_archive_files_count()
            rejected_files = _get_rejected_files_count()
            workdir_files = _get_workdir_files_count()

            """ Get CSW records and SolR docs"""
            csw_records = csw_getCount(self.csw_connection)
            solr_docs, solr_current = _get_solr_count(self.mysolr.solr_url,
                                                      self.solr_auth)
            if jobdata['current_task_id'] is not None:
                task = rebuild_task.AsyncResult(jobdata['current_task_id'])
                logger.debug(task.state)
                if task.state == 'FAILURE' or task.state == 'SUCCESS':
                    jobdata['previous_task_status'] = str(task.state)
                    jobdata['previous_task_status'] += " : " + str(task.info)
                    # jobdata['current_task_id'] = None

            return render_template("admin.html",
                                   archive_files=archive_files,
                                   csw_records=csw_records,
                                   solr_docs=solr_docs,
                                   solr_current=solr_current,
                                   rejected_files=rejected_files,
                                   workdir_files=workdir_files,
                                   current_task=jobdata['current_task_id'],
                                   prev_task_status=jobdata['previous_task_status'],
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
                jobdata['current_task_id'] = task.id

            except Exception as e:
                message = "Somthing went wrong running task. is redis and celery running? "
                return message + e, 500

            """Start background daemon collection results"""
            resultDaemon = Thread(target=process_results)
            resultDaemon.daemon = True
            resultDaemon.start()

            """Register rebuild start date"""
            now = datetime.now()
            last_rebuild = now.strftime("%d/%m/%Y, %H:%M:%S")
            jobdata['last_rebuild'] = last_rebuild
            jobdata['failed_statis_report'] = dict()
            return jsonify({}), 202, {'Location': url_for('rebuild_taskstatus',
                                      task_id=task.id)}
            # return "OK", 200

        # POST Rebuild catalog.
        @self.route("/rebuild/cancel", methods=["POST"])
        @self.auth.login_required
        def cancel_rebuild():
            response = {'state': 'REVOKED',
                        'current': jobdata['current'],
                        'total': jobdata['total'],
                        'status': 'Manually canceled',  # this is the exception raised
                        }
            if jobdata['current_task_id'] is not None:
                app.control.purge()
                task = rebuild_task.AsyncResult(jobdata['current_task_id'])
                task.revoke(terminate=True)
                logger.debug(task.state)
                for ingest_tasks in task.children:
                    _revoke_tasks(ingest_tasks)
                #         ingest_task.revoke(terminate=True)
                task.revoke(terminate=True)
                jobdata['previous_task_status'] = "Revoked" + " : " + "Manually canceled."
                jobdata['current_task_id'] = None

            return jsonify(response)

        @self.route('/rebuild/result')
        def rebuild_result():
            response_obj = dict()
            response_obj['previous_task_status'] = jobdata['previous_task_status']
            result_dict = jobdata['results']
            if 'current_task_id' in jobdata and jobdata['current_task_id'] is not None:
                task_id = jobdata['current_task_id']
                task = rebuild_task.AsyncResult(task_id)
                response_obj['current'] = task.info.get('total', 0),
                response_obj['total'] = task.info.get('total', 1),
                response_obj['status'] = task.info.get('status', '')
                response_obj['state'] = task.state

            else:
                response_obj['state'] = 'Not running'
            response_obj['rebuild_start_time'] = jobdata['last_rebuild']
            response_obj['rebuild_summary'] = jobdata['last_rebuild_info']
            response_obj['failed_status_report'] = result_dict
            return jsonify(response_obj), 200

        @self.route('/status/<task_id>')
        def rebuild_taskstatus(task_id):
            task = rebuild_task.AsyncResult(task_id)
            if task is None:
                return jsonify({"Job not running...See logs"})
            else:
                logger.debug(task.state)

                if task.state == 'PENDING':
                    response = {
                        'state': task.state,
                        'current': 0,
                        'total': 1,
                        'status': task.info.get('status', '')

                    }
                elif task.state == 'REVOKED':
                    response = {
                        'state': task.state,
                        'current': 0,
                        'total': 1,
                        'status': "Manually revoked task"

                    }

                elif task.state == 'SUCCESS':
                    # jobdata['current_task_id'] = None
                    response = {
                        'state': task.state,
                        'current': task.info.get('total', 0),
                        'total': task.info.get('total', 1),
                        'status': task.info.get('status', '')

                    }
                    if 'result' in task.info:
                        response['result'] = task.info['result']
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

        def _get_solr_count(solr_url, authentication):
            """Get SOLR documents"""
            solr_status = get_solrstatus(solr_url, authentication)
            solr_docs = solr_status['numDocs']
            solr_current = solr_status['current']
            return solr_docs, solr_current

        def _revoke_tasks(tasks):
            """recursive task revoke function"""
            for task in tasks:
                if isinstance(task, GroupResult):
                    task.revoke()
                elif isinstance(task, ResultBase):
                    task.revoke()
                elif isinstance(task, list):
                    _revoke_tasks(task)
                else:
                    pass

        def process_results():
            """Process task results as they are completed"""
            result_dict = dict()
            results_processed = False
            while True:
                task = None
                try:
                    task = rebuild_task.AsyncResult(jobdata['current_task_id'])
                    logger.debug("Main task id: %s", task.id)
                    logger.debug("Main task state %s", task.state)
                    if task.state == 'PROGRESS':
                        logger.debug("New running task..set collect-status as false")
                        results_processed = False
                        jobdata['results'] = dict()
                except Exception:
                    logger.info("No running task...waiting...")
                    pass
                if task is not None:
                    parentJobId = None
                    try:
                        parentJobId = task.info.get('parent_job_id', None)
                        logger.debug("Got parent job id: %s", parentJobId)
                        parentJob = dmci_dist_ingest_task.AsyncResult(parentJobId)
                        logger.info("Found parent mmd job task. Collecting results")
                        logger.debug(type(parentJob))
                        # for t in parentJob.collect():
                        #  logger.debug(type(t))
                        for t in parentJob.children:
                            logger.debug(type(t))

                    except Exception:
                        pass

                    mmdJobId = None
                    try:
                        mmdJobId = task.info.get('mmd_job_id', None)
                        logger.debug("Got mmd job id: %s", mmdJobId)
                        mmdJob = dmci_dist_ingest_task.AsyncResult(mmdJobId)
                        logger.info("Found parent mmd job task. Collecting results")
                        logger.debug(type(mmdJob))
                        # for t in mmdJob.collect():
                        #     logger.debug(type(t))
                        for t in mmdJob.children:
                            logger.debug(type(t))

                    except Exception:
                        pass

                    if task.ready() and results_processed is False:
                        logger.debug("Main task finished. Collecting results....")
                        for ingest_task in task.children:
                            if isinstance(ingest_task, list):
                                for t in ingest_task:
                                    logger.debug(t.name)
                                    logger.debug(t.args)

                            if isinstance(ingest_task, GroupResult):
                                for t in ingest_task.children:
                                    # logger.debug(t.get())
                                    status, file, msg = t.get()
                                    if status is False:
                                        result_dict[file] = msg
                        jobdata['results'] = result_dict
                        jobdata['last_rebuild_info'] = task.info.get('status', '')
                        results_processed = True
                logger.debug("Sleeeping and check for new job")
                sleep(10)
                logger.debug("End while")
            # if task.successful():
            #     # logger.debug(task.children)
            #     for ingest_task in task.children:
            #         if isinstance(ingest_task, list):
            #             for t in ingest_task:
            #                 logger.debug(t.name)
            #                 logger.debug(t.args)

            #         if isinstance(ingest_task, GroupResult):
            #             for t in ingest_task.children:
            #                 # logger.debug(t.get())
            #                 status, file, msg = t.get()
            #                 if status is False:
            #                     result_dict[file] = msg
            #                     # file = msg.split('XZZZX')[0]
            #                     # reason = msg.split('XZZZX')[1]
            #                     # result_dict[file] = reason

        def _recurse_results(tasks):
            """recursive task result loop function function"""
            for task in tasks:
                if isinstance(task, GroupResult):
                    if task.successful():
                        status, file, msg = task.get()
                        logger.debug("%s: %s: %s", status, file, msg)
                elif isinstance(task, ResultBase):
                    if task.successful():
                        status, file, msg = task.get()
                        logger.debug("%s: %s: %s", status, file, msg)
                elif isinstance(task, list):
                    _revoke_tasks(task)
                else:
                    pass
