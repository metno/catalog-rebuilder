# catalog-rebuilder

Catalog rebuilder of CSW and Solr for the S-ENDA project

![Catalog rebuilder component diagram](rebuilder-component-diagram.png)

## Environment Variables

The package reads the following environment variables.

* `CATALOG_REBUILDER_ENABLED` If `true` or `True` the catalog will be rebuilt. If not set or other values, rebuilding will not execute.
* `MMD_ARCHIVE_PATH` the local path to look for the **mmd-archive**.
* `DMCI_REBUILDER_URL` the url for the custom rebuilder instance of DMCI.
* `PARENT_UUID_LIST`the full path to the parent-uuid-list.xml. If not provided,
it will look for the file in /parent-uuid-list.xml
* `DEBUG` set this to other than blank to enable debugging

## DMCI Sidecar

The catalog-rebuilder will use its own dmci sidecar for ingestion. The DMCI can then be tailored for which distributors
and enpoints (SolR, pycsw) to use for the ingestion.

## License

Copyright 2021 MET Norway

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
compliance with the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License
is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing permissions and limitations under the
License.

See Also: [LICENSE](https://raw.githubusercontent.com/metno/catalog-rebuilder/main/LICENSE)
