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
import json
import logging
import psycopg2
from time import sleep
from threading import Thread
from datetime import datetime

from catalog_rebuilder import rebuild_task, getListOfFiles, app, dmci_dist_ingest_task
from catalog_tools import csw_getCount, rejected_delete, get_solrstatus
from catalog_tools import csw_truncateRecords, get_xml_file_count
from catalog_tools import countParentUUIDList, csw_getParentCount, csw_getDistinctParentsCount
from catalog_tools import get_solrParentCount, get_unique_parent_refs

from flask import Flask, render_template, request, url_for, redirect, jsonify
from flask import send_from_directory
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash
from requests.auth import HTTPBasicAuth as BasicAuth
from solrindexer import IndexMMD

from celery.result import GroupResult, ResultBase, AsyncResult
from main import CONFIG, jobdata, resultData, catalogStatus

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
    """
    The main flask app for the catalog rebuilder with status and admin endpoints
    """

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
        self.pg_port = os.environ.get("PG_CSW_PORT", CONFIG.csw_postgis_port)
        # db_url = "postgis-operator"
        self.db_url = os.environ.get("PG_CSW_DB_URL", CONFIG.csw_postgis_host)
        # logger.info("Connectiong to pycsw postgis: %s", db_url)
        self.csw_connection = None
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
            self.csw_connection = _get_pg_connection()
            self.csw_connection.autocommit = True
            logger.debug(self.template_folder)
            # Start background thread for updating catalog integrity information
            """Start background daemon collection results"""
            catalogDaemon = Thread(target=catalog_status)
            catalogDaemon.daemon = True
            catalogDaemon.start()
            # if catalogStatus['archive'] == 0:
            #    archive_files = _get_archive_files_count()
            # else:
            #    archive_files = catalogStatus['archive']

            rejected_files = _get_rejected_files_count()
            workdir_files = _get_workdir_files_count()
            if catalogStatus['parent-uuid-list'] == 0:
                parent_uuid_list = _get_parent_uuid_list_count()
            else:
                parent_uuid_list = catalogStatus['parent-uuid-list']

            """ Get CSW records"""
            if catalogStatus['csw'] == 0:
                csw_records = csw_getCount(_get_pg_connection())
            else:
                csw_records = catalogStatus['csw']

            if catalogStatus['csw-marked-parents'] == 0:
                csw_parent_count = csw_getParentCount(_get_pg_connection())
            else:
                csw_parent_count = catalogStatus['csw-marked-parents']

            if catalogStatus['csw-distinct-parents'] == 0:
                csw_distinct_parent_ids = csw_getDistinctParentsCount(_get_pg_connection())
            else:
                csw_distinct_parent_ids = catalogStatus['csw-distinct-parents']

            solr_docs, solr_current = _get_solr_count(self.mysolr.solr_url,
                                                      self.solr_auth)
            if catalogStatus['solr-marked-parents'] == 0:
                solr_parent_count = _get_solr_parent_count(self.mysolr.solr_url,
                                                           self.solr_auth)
            else:
                solr_parent_count = catalogStatus['solr-marked-parents']

            if catalogStatus['solr-unique-parents'] == 0:
                solr_parent_unique = _get_solr_parent__refs_count(self.mysolr.solr_url,
                                                                  self.solr_auth)
            else:
                solr_parent_unique = catalogStatus['solr-unique-parents']

            self.csw_connection.close()
            return render_template("status.html",
                                   archive_files=0,
                                   csw_records=csw_records,
                                   solr_docs=solr_docs,
                                   solr_current=solr_current,
                                   rejected_files=rejected_files,
                                   workdir_files=workdir_files,
                                   parent_uuid_list=parent_uuid_list,
                                   csw_parent_count=csw_parent_count,
                                   csw_distinct_parent_ids=csw_distinct_parent_ids,
                                   solr_parent_count=solr_parent_count,
                                   solr_parent_unique=solr_parent_unique
                                   )

        # Show catalog status
        @self.route("/admin")
        @self.auth.login_required
        def admin():
            """Simple form with buttons to manage rejected files and rebuild catalog.
            Form will have buttons that execute the below POST endpoints"""
            """Global job result dict"""
            self.csw_connection = _get_pg_connection()
            self.csw_connection.autocommit = True
            """Get DMCI info"""
            #if catalogStatus['archive'] == 0:
            #    archive_files = _get_archive_files_count()
            #else:
            #    archive_files = catalogStatus['archive']
            rejected_files = _get_rejected_files_count()
            workdir_files = _get_workdir_files_count()
            if catalogStatus['parent-uuid-list'] == 0:
                parent_uuid_list = _get_parent_uuid_list_count()
            else:
                parent_uuid_list = catalogStatus['parent-uuid-list']

            """ Get CSW records and SolR docs"""
            """ Get CSW records"""
            if catalogStatus['csw'] == 0:
                csw_records = csw_getCount(_get_pg_connection())
            else:
                csw_records = catalogStatus['csw']

            if catalogStatus['csw-marked-parents'] == 0:
                csw_parent_count = csw_getParentCount(_get_pg_connection())
            else:
                csw_parent_count = catalogStatus['csw-marked-parents']

            if catalogStatus['csw-distinct-parents'] == 0:
                csw_distinct_parent_ids = csw_getDistinctParentsCount(_get_pg_connection())
            else:
                csw_distinct_parent_ids = catalogStatus['csw-distinct-parents']

            solr_docs, solr_current = _get_solr_count(self.mysolr.solr_url,
                                                      self.solr_auth)
            if catalogStatus['solr-marked-parents'] == 0:
                solr_parent_count = _get_solr_parent_count(self.mysolr.solr_url,
                                                           self.solr_auth)
            else:
                solr_parent_count = catalogStatus['solr-marked-parents']

            if catalogStatus['solr-unique-parents'] == 0:
                solr_parent_unique = _get_solr_parent__refs_count(self.mysolr.solr_url,
                                                                  self.solr_auth)
            else:
                solr_parent_unique = catalogStatus['solr-unique-parents']
            if jobdata['current_task_id'] is not None:
                task = rebuild_task.AsyncResult(jobdata['current_task_id'])
                logger.debug(task.state)
                if task.state == 'FAILURE' or task.state == 'SUCCESS':
                    jobdata['previous_task_status'] = str(task.state)
                    jobdata['previous_task_status'] += " : " + str(task.info)
                    # jobdata['current_task_id'] = None

            # self.csw_connection.close()
            return render_template("admin.html",
                                   archive_files=0,
                                   csw_records=csw_records,
                                   solr_docs=solr_docs,
                                   solr_current=solr_current,
                                   parent_uuid_list=parent_uuid_list,
                                   csw_parent_count=csw_parent_count,
                                   csw_distinct_parent_ids=csw_distinct_parent_ids,
                                   solr_parent_count=solr_parent_count,
                                   solr_parent_unique=solr_parent_unique,
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

        # POST Clean workdir folder
        @self.route("/workdir/clean", methods=["POST"])
        @self.auth.login_required
        def clean_workdir():
            """ Clean / Delete all files in workdir folder"""
            status, msg = rejected_delete(self._conf.distributor_cache)
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
            self.csw_connection = _get_pg_connection()
            self.csw_connection.autocommit = True
            """ clean / delete pycsw"""
            status, msg = csw_truncateRecords(self.csw_connection)
            referrer = request.referrer
            self.csw_connection.close()
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
                # pid = open('rebuild.pid', 'w')
                # pid.write(task.id)
                # pid.close()

            except Exception as e:
                message = "Something went wrong running task. is redis and celery running? "
                return message + e, 500

            """Start background daemon collection results"""
            resultDaemon = Thread(target=process_results)
            resultDaemon.daemon = True
            resultDaemon.start()

            """Register rebuild start date"""
            now = datetime.now()
            last_rebuild = now.strftime("%d/%m/%Y, %H:%M:%S")
            jobdata['last_rebuild'] = last_rebuild
            jobdata['failed_statis_report'] = None
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
                # logger.debug(task.state)
                if task is not None:
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
            # result_dict = jobdata['results']
            if 'current_task_id' in jobdata and jobdata['current_task_id'] is not None:
                task_id = jobdata['current_task_id']
                task = rebuild_task.AsyncResult(task_id)
                if task is not None:
                    try:
                        response_obj['current'] = task.info.get('current', 0),
                        response_obj['total'] = task.info.get('total', 1),
                        response_obj['status'] = task.info.get('status', '')
                        response_obj['state'] = task.state
                    except Exception:
                        pass

            else:
                response_obj['state'] = 'Not running'
            response_obj['rebuild_start_time'] = jobdata['last_rebuild']
            response_obj['rebuild_summary'] = jobdata['last_rebuild_info']
            response_obj['failed_status_report'] = resultData.copy()
            return jsonify(response_obj), 200

        @self.route('/rebuild/report')
        def rebuild_report():
            # try:
            #     data = open('/repo/rebuild-report.json').read()
            #     res_dict = json.loads(data)
            # except Exception:
            #     return jsonify({})
            # return jsonify(res_dict)
            return send_from_directory(self._conf.report_path,
                                       'rebuild-report.json', as_attachment=True)

        @self.route('/status/<task_id>')
        def rebuild_taskstatus(task_id):
            task = rebuild_task.AsyncResult(task_id)
            if task is None:
                return jsonify({"Job not running...See logs"})
            else:
                logger.debug(task.state)

                if task.state == 'PENDING':
                    if task.info is not None:
                        task_status = task.info.get('status', '')
                    else:
                        task_status = ''
                    response = {
                        'state': task.state,
                        'current': 0,
                        'total': 1,
                        'status': task_status

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
                logger.debug(response)
                return jsonify(response)

        def _get_pg_connection():
            conn = psycopg2.connect(host=self.db_url,
                                    user=self.csw_username,
                                    password=self.csw_password,
                                    dbname='csw_db', port=self.pg_port)
            return conn

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
            """Get SOLR documents count"""
            solr_status = get_solrstatus(solr_url, authentication)
            solr_docs = solr_status['numDocs']
            solr_current = solr_status['current']
            return solr_docs, solr_current

        def _get_solr_parent_count(solr_url, authentication):
            """Get SOLR parent count"""
            solr_status = get_solrParentCount(solr_url, authentication)
            # solr_docs = solr_status['numDocs']
            # solr_current = solr_status['current']
            return solr_status

        def _get_solr_parent__refs_count(solr_url, authentication):
            """Get SOLR parent count"""
            solr_status = get_unique_parent_refs(solr_url, authentication)
            # solr_docs = solr_status['numDocs']
            # solr_current = solr_status['current']
            return solr_status

        def _get_parent_uuid_list_count():
            parent_list = self._conf.path_to_parent_list
            return countParentUUIDList(parent_list)

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

        def catalog_status():
            """Function executed by daemon thread.
            Regulary check the catalog integrity"""
            while True:
                logger.debug("Collecting catalog integrity...")
                self.csw_connection = _get_pg_connection()
                self.csw_connection.autocommit = True
                logger.debug(self.template_folder)
                #archive_files = _get_archive_files_count()
                parent_uuid_list = _get_parent_uuid_list_count()
                """ Get CSW records"""
                csw_records = csw_getCount(_get_pg_connection())
                csw_parent_count = csw_getParentCount(_get_pg_connection())
                csw_distinct_parent_ids = csw_getDistinctParentsCount(_get_pg_connection())

                solr_docs, solr_current = _get_solr_count(self.mysolr.solr_url,
                                                          self.solr_auth)
                solr_parent_count = _get_solr_parent_count(self.mysolr.solr_url,
                                                           self.solr_auth)
                solr_parent_unique = _get_solr_parent__refs_count(self.mysolr.solr_url,
                                                                  self.solr_auth)

                catalogStatus['archive'] = 0
                catalogStatus['csw'] = csw_records
                catalogStatus['solr'] = solr_docs
                catalogStatus['solr-current'] = solr_current
                catalogStatus['parent-uuid-list'] = parent_uuid_list
                catalogStatus['csw-marked-parents'] = csw_parent_count
                catalogStatus['csw-distinct-parents'] = csw_distinct_parent_ids
                catalogStatus['solr-marked-parents'] = solr_parent_count
                catalogStatus['solr-unique-parents'] = solr_parent_unique
                self.csw_connection.close()
                sleep(15)

        def process_results():
            """Process task results as they are completed
            Run as daemon thread"""
            # result_dict = dict()
            results_processed = False
            while True:
                task = None
                try:
                    task = rebuild_task.AsyncResult(jobdata['current_task_id'])
                    logger.debug("Main task id: %s", task.id)
                    logger.debug("Main task state %s", task.state)
                    logger.debug("Main task status %s", task.status)
                    current = task.info.get('current', None)
                    logger.debug("main task current: %s", current)
                    if task.state == 'PENDING' and current == 0 or current is None:
                        logger.debug("New running task..set collect-status as false")
                        results_processed = False
                        # jobdata['results'] = result_dict

                    if task.state == 'PROGRESS' and current is not None:
                        logger.debug("Harvesting main task results...")
                        for t in task.children:
                            _recurse_results(t)

                except Exception:
                    logger.info("No running  main task...waiting...")
                    sleep(30)
                    pass
                if task is not None:
                    parentJobId = None

                    try:
                        parentJobId = task.info.get('parent_job_id', None)
                        parentJob = AsyncResult(parentJobId, app=app)
                        logger.debug("Got parent job id: %s", parentJob.id)
                        logger.debug("Parent job state %s", parentJob.state)
                        logger.debug("Parent job status %s", parentJob.status)
                        if parentJob.state == 'PROGRESS' and current is not None:
                            logger.debug("Harvesting parent tasks results...")
                            for t in parentJob.children:
                                _recurse_results(t)

                    except Exception:
                        pass

                    mmdJobId = None
                    try:
                        mmdJobId = task.info.get('mmd_job_id', None)
                        mmdJob = dmci_dist_ingest_task.AsyncResult(mmdJobId)
                        logger.debug("Got mmd job id: %s", mmdJob.id)
                        logger.debug("mmd job state %s", mmdJob.state)
                        logger.debug("mmd job status %s", mmdJob.status)
                        # logger.debug(type(mmdJob))
                        if mmdJob.state == 'PROGRESS' and current is not None:
                            logger.debug("Harvesting mmd tasks results...")
                            for t in task.children:
                                _recurse_results(t)

                        # for result, value in mmdJob.collect(intermediate=True):
                        #     logger.debug("mmd task result: %s, mmd task value: %s",
                        #  result, value)
                        # if mmdJob is not None:
                        #     for tasks in mmdJob.collect(intermediate=True):
                        #         _recurse_results(tasks)

                    except Exception:
                        pass

                    if task is not None and task.ready() and results_processed is False:
                        logger.debug("Main task finished. Collecting results....")
                        for ingest_task in task.children:
                            if isinstance(ingest_task, list):
                                for t in ingest_task:
                                    logger.debug(t.name)
                                    logger.debug(t.args)

                            if isinstance(ingest_task, GroupResult):
                                for t in ingest_task.children:
                                    # logger.debug(t.get())
                                    try:
                                        status, file, msg = t.get()
                                        if status is False:
                                            resultData.update({file: msg})
                                    except Exception as e:
                                        logger.error("Failed to get job result from redis: %s", e)
                                        pass
                        # jobdata['results'] = result_dict
                        jobdata['last_rebuild_info'] = task.info.get('status', '')
                        results_processed = True
                        # logger.debug(resultData)
                        if task is not None:
                            try:
                                jobdata['current'] = task.info.get('current', 0),
                                jobdata['total'] = task.info.get('total', 1),
                                jobdata['status'] = task.info.get('status', '')
                                jobdata['state'] = task.state
                                jobdata['failed'] = len(dict(resultData))
                            except Exception:
                                pass
                        task.forget()
                        task = None
                        resultReport = {}
                        resultReport['jobdata'] = dict(jobdata)
                        resultReport['results'] = dict(resultData)
                        json_obj = json.dumps(resultReport, indent=4)
                        # for item in json_obj:
                        #     for key, value in item:
                        #         item[key] = value.strip()
                        logger.debug("Writing report file to %s",
                                     self._conf.report_path+'/rebuild-report.json')
                        with open(os.path.join(self._conf.report_path,
                                               "rebuild-report.json"), "w") as final:
                            final.write(json_obj)
                        logger.debug("File written...")
                        jobdata['current_task_id'] = None

                if task is not None:
                    logger.debug("Waiting for task %s to finish. current state: %s",
                                 task.id, task.state)
                else:
                    logger.debug("Sleeeping and check for new job")
                sleep(15)
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
                    if task.status == "SUCCESS":
                        status, file, msg = task.get()
                        if status is False:
                            # logger.debug("%s: %s: %s", status, file, msg)
                            resultData[file] = msg

                elif isinstance(task, ResultBase):
                    if task.status == "SUCCESS":
                        status, file, msg = task.get()
                        if status is False:
                            # logger.debug("%s: %s: %s", status, file, msg)
                            resultData[file] = msg

                elif isinstance(task, list):
                    _recurse_results(task)
                elif isinstance(task, str):
                    # logger.debug(task)
                    t = AsyncResult(task, app=app)
                    if t.status == "SUCCESS":
                        status, file, msg = t.get()
                        if status is False:
                            # logger.debug("%s: %s: %s", status, file, msg)
                            resultData[file] = msg
                else:
                    # logger.debug(type(task))
                    pass
