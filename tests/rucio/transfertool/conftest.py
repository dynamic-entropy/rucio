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

"""Give transfertool unit tests a config file before they import server modules.

``pytest tests/rucio`` does not start a database. Importing the transfer tool
loads the schema package, which requires a configuration file to exist.
Client mode keeps those imports from opening a database connection.
"""

import os
import tempfile

if "RUCIO_CONFIG" not in os.environ:
    _handle = tempfile.NamedTemporaryFile(mode="w", prefix="rucio-rta-", suffix=".cfg", delete=False)
    _handle.write("[common]\nmulti_vo = False\n\n[client]\n")
    _handle.close()
    os.environ["RUCIO_CONFIG"] = _handle.name
