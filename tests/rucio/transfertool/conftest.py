# Copyright European Organization for Nuclear Research (CERN) since 2012
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Keep transfertool unit tests off the database.

``pytest tests/rucio`` does not start a database. Importing the transfer tool
loads the schema package, which requires a configuration file to exist.
``RUCIO_CLIENT_MODE`` makes ``config_get`` return its default instead of
opening a session. Without that, a config cache left by an earlier test
(no ``[client]`` section) sends ``_scitags_ids`` and ``core.transfer`` at
the missing ``[database]`` section.
"""

import os
import tempfile

os.environ.setdefault("RUCIO_CLIENT_MODE", "1")

if "RUCIO_CONFIG" not in os.environ:
    _handle = tempfile.NamedTemporaryFile(mode="w", prefix="rucio-rta-", suffix=".cfg", delete=False)
    _handle.write("[common]\nmulti_vo = False\n\n[client]\n")
    _handle.close()
    os.environ["RUCIO_CONFIG"] = _handle.name
