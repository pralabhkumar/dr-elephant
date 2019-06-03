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

/**
 * This query make neccesssary steps to run pig hbt
 */


INSERT INTO `tuning_parameter` VALUES (28,'mapreduce.task.io.sort.mb',4,100,50,1920,50,0,current_timestamp(0), current_timestamp(0));
INSERT INTO `tuning_parameter` VALUES (29,'mapreduce.task.io.sort.factor',4,10,10,150,10,0,current_timestamp(0), current_timestamp(0));
INSERT INTO `tuning_parameter` VALUES (30,'mapreduce.map.sort.spill.percent',4,0.8,0.6,0.95,0.1,0,current_timestamp(0), current_timestamp(0));
INSERT INTO `tuning_parameter` VALUES (31,'mapreduce.reduce.memory.mb',4,2048,1024,8192,1024,0,current_timestamp(0), current_timestamp(0));
INSERT INTO `tuning_parameter` VALUES (32,'pig.maxCombinedSplitSize',4,536870912,536870912,536870912,128,0,current_timestamp(0), current_timestamp(0));
INSERT INTO `tuning_parameter` VALUES (33,'mapreduce.reduce.java.opts',4,1536,500,6144,64,0,current_timestamp(0), current_timestamp(0));
INSERT INTO `tuning_parameter` VALUES (34,'mapreduce.input.fileinputformat.split.maxsize',4,536870912,536870912,536870912,128,0,current_timestamp(0), current_timestamp(0));
INSERT INTO `tuning_parameter` VALUES (35,'mapreduce.job.reduces',4,1,1,999,10,0,current_timestamp(0), current_timestamp(0));

/**
 * This query make neccesssary steps to make job_definition_id unique in tuning_job_definition
 */

ALTER TABLE tuning_job_definition ADD PRIMARY KEY (job_definition_id);



# --- !Downs
DELETE FROM tuning_parameter WHERE tuning_algorithm_id=28;
DELETE FROM tuning_parameter WHERE tuning_algorithm_id=29;
DELETE FROM tuning_parameter WHERE tuning_algorithm_id=30;
DELETE FROM tuning_parameter WHERE tuning_algorithm_id=31;
DELETE FROM tuning_parameter WHERE tuning_algorithm_id=32;
DELETE FROM tuning_parameter WHERE tuning_algorithm_id=33;
DELETE FROM tuning_parameter WHERE tuning_algorithm_id=34;
DELETE FROM tuning_parameter WHERE tuning_algorithm_id=35;
ALTER TABLE tuning_job_definition DROP PRIMARY KEY;
