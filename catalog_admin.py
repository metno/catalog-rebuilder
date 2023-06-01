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

import catalog_rebuilder as cb


from flask import Flask, render_template
from requests.auth import HTTPBasicAuth

auth = HTTPBasicAuth()

app = Flask(__name__)


# Show catalog status
@app.route("/status")
def status():
    """ Return integrity status of the catalog. Include number of rejected files"""
    status = cb.check_integrety
    return render_template("status.html", status=status)


# Show catalog status
@app.route("/admin")
@auth.login_required
def admin():
    """Simple form with buttons to manage rejected files and rebuild catalog.
    Form will have buttons that execute the below POST endpoints"""
    return render_template("admin.html")


# List Rejected
@app.route("/rejected")
def list_rejected():
    """ List files in rejected direcotry. use maybe browsepy or similar
    """

    return NotImplementedError


# GET All rejected as a zip file
@app.route("/rejected/get")
def get_rejected():
    """"Create a zip file of all contents in rejected folder and return"""
    return NotImplementedError


# POST Clean rejected folder
@app.route("/rejected/clean", methods=["POST"])
@auth.login_required
def clean_rejected():
    """ Clean / Delete all files in rejected folder"""
    return NotImplementedError


# POST Clean solr index
@app.route("/solr/clean", methods=["POST"])
@auth.login_required
def clean_solr():
    """ clean / delete solr index"""
    return NotImplementedError


# POST Clean solr index
@app.route("/pycsw/clean", methods=["POST"])
def clean_pycsw():
    """ clean / delete pycsw"""
    return NotImplementedError


# POST Rebuild catalog.
@app.route("/rebild", methods=["POST"])
@auth.login_required
def rebuild_catalog():
    """ Rebuld the catalog. given the

    Input parameters:
     dist : list - list of distributors [solr,pycsw]
    """
    return NotImplementedError


app.run(port=5000, debug=True)
