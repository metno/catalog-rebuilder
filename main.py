#!/usr/bin/env python3

"""
Catalog rebuilder admin interface - main docker entry point function
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
import re
import sys
from werkzeug.middleware.dispatcher import DispatcherMiddleware

from dmci import Config

from multiprocessing import Manager

"""Bootstrap thread-proof dict to store tasi res7otvalues"""
jobdata = Manager().dict()
jobdata['current_task_id'] = None
jobdata['previous_task_status'] = None
jobdata['total'] = 0
jobdata['current'] = 0
jobdata['failed'] = 0
jobdata['results'] = None
jobdata['last_rebuild'] = ''
jobdata['last_rebuild_info'] = None
jobdata['running'] = None

resultData = Manager().dict()

catalogStatus = Manager().dict()
catalogStatus['archive'] = 0
catalogStatus['csw'] = 0
catalogStatus['solr'] = 0
catalogStatus['solr-current'] = None
catalogStatus['parent-uuid-list'] = 0
catalogStatus['csw-marked-parents'] = 0
catalogStatus['csw-distinct-parents'] = 0
catalogStatus['solr-marked-parents'] = 0
catalogStatus['solr-unique-parents'] = 0
catalogStatus['csw-parents-check'] = None
catalogStatus['running'] = None


# # App Initialization
# def initialize():
#     global jobdata
#     jobdata = {}
#     manager_dict = Manager().dict()
#     manager_dict['manager_key'] = 'manager_value'
#     jobdata['multiprocess_manager'] = manager_dict
#     return

class CRConfig(Config):
    """Override standard DMCI config object to add more configurations"""
    def __init__(self):
        super().__init__()
        self.pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))

        # Catalog rebuilder specific configuration
        self.csw_postgis_host = None  # The url/hostname for the postgis-server
        self.csw_postgis_port = 5432  # The user credentials for the postgis-server
        self.csw_postgis_user = None  # The user credentials for the postgis-server

        self.csw_postgis_password = None  # The password credentials for the postgis-server

        self.mmd_repo_path = None  # Where to clone/pull the MMD repo used for rebuilding
        self.mmd_repo_url = None  # The url for which MMD repo to clone/pull
        self.mmd_repo_user = None  # The user credentials for MMD repo
        self.mmd_repo_password = None  # The password credentials for MMD repo

        self.redis_broker = None  # Url for redis-broker

        self.browsepy_base_dir = None  # Start dir for browsepy

        self.report_path = '/repo/'
        return

    def readConfig(self, configFile=None):
        valid = super().readConfig(configFile=configFile)
        """Read config values under 'catalog-rebuilder'."""
        conf = self._raw_conf.get("catalog-rebuilder", {})
        self.csw_postgis_host = conf.get("csw_postgis_host", self.csw_postgis_host)
        self.csw_postgis_port = conf.get("csw_postgis_port", self.csw_postgis_port)
        self.csw_postgis_user = conf.get("csw_postgis_user", self.csw_postgis_user)
        self.csw_postgis_password = conf.get("csw_postgis_password", self.csw_postgis_password)

        self.mmd_repo_path = conf.get("mmd_repo_path", self.mmd_repo_path)
        self.mmd_repo_url = conf.get("mmd_repo_url", self.mmd_repo_url)
        self.mmd_repo_user = conf.get("mmd_repo_user", self.mmd_repo_user)
        self.mmd_repo_password = conf.get("mmd_repo_password", self.mmd_repo_password)

        "Process mmd repo url add credentials to string"
        if self.mmd_repo_user is not None and self.mmd_repo_password is not None:
            mmd_url = re.sub('%u', self.mmd_repo_user, self.mmd_repo_url)
            mmd_url = re.sub('%p', self.mmd_repo_password, mmd_url)
            self.mmd_repo_url = mmd_url

        self.redis_broker = conf.get("redis_broker", self.redis_broker)

        self.browsepy_base_dir = conf.get("browsepy_base_dir", self.browsepy_base_dir)

        self.report_path = conf.get('report_path', self.report_path)
        return valid


"""Bootstrap DCMI Config Object"""
CONFIG = CRConfig()


def main():
    """This is the main entry point."""
    from catalog_flask import AdminApp
    os.curdir = os.path.abspath(os.path.dirname(__file__))
    if not CONFIG.readConfig(configFile=os.environ.get("DMCI_CONFIG", None)):
        sys.exit(1)
    # initialize()
    app = AdminApp()
    sys.exit(app.run(port=5000, debug=True))


def create_app():
    """This is the wsgi entry point."""
    from catalog_flask import AdminApp
    from browsepy import app as browsepyApp

    if not CONFIG.readConfig(configFile=os.environ.get("DMCI_CONFIG", None)):
        sys.exit(1)
    browsepyApp.config.update(APPLICATION_ROOT='/browsepy',
                              directory_base=CONFIG.browsepy_base_dir,
                              directory_remove=None,
                              directory_downloadable=True,
                              )
    # initialize()
    rebuilderApp = AdminApp()
    app = DispatcherMiddleware(rebuilderApp, {'/dmci': browsepyApp})

    # app = AdminApp()
    return app


if __name__ == '__main__':
    os.curdir = os.path.abspath(os.path.dirname(__file__))
    main()
