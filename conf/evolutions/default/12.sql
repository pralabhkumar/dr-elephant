#
# Copyright 2016 LinkedIn Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

# --- !Ups

ALTER TABLE exception_fingerprinting ADD COLUMN log_source_information varchar(1000) AFTER exception_log;
ALTER TABLE exception_fingerprinting MODIFY exception_log varchar(10000);


# --- !Downs

ALTER TABLE exception_fingerprinting DROP COLUMN log_source_information;
ALTER TABLE exception_fingerprinting ALTER COLUMN exception_log varchar(5120)


