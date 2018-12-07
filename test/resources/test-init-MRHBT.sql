insert into yarn_app_result
    (id,name,username,queue_name,start_time,finish_time,tracking_url,job_type,severity,score,workflow_depth,scheduler,job_name,job_exec_id,flow_exec_id,job_def_id,flow_def_id,job_exec_url,flow_exec_url,job_def_url,flow_def_url,resource_used,resource_wasted,total_delay) values
    ('application_1458194917883_1453361','Email Overwriter','growth','misc_default',1460980616502,1460980723925,'http://elephant.linkedin.com:19888/jobhistory/job/job_1458194917883_1453361','HadoopJava',0,0,0,'azkaban','overwriter-reminder2','https://ltx1-holdemaz01.grid.linkedin.com:8443/executor?execid=5416293&job=countByCountryFlow_countByCountry&attempt=0','https://ltx1-holdemaz01.grid.linkedin.com:8443/executor?execid=5416293','https://ltx1-holdemaz01.grid.linkedin.com:8443/manager?project=AzkabanHelloPigTest&flow=countByCountryFlow&job=countByCountryFlow_countByCountry','https://elephant.linkedin.com:8443/manager?project=b2-confirm-email-reminder&flow=reminder','https://elephant.linkedin.com:8443/executor?execid=1654676&job=overwriter-reminder2&attempt=0','https://elephant.linkedin.com:8443/executor?execid=1654676','https://elephant.linkedin.com:8443/manager?project=b2-confirm-email-reminder&flow=reminder&job=overwriter-reminder2','https://elephant.linkedin.com:8443/manager?project=b2-confirm-email-reminder&flow=reminder', 100, 30, 20),
    ('application_1458194917883_1453362','Email Overwriter','metrics','misc_default',1460980823925,1460980923925,'http://elephant.linkedin.com:19888/jobhistory/job/job_1458194917883_1453362','HadoopJava',0,0,0,'azkaban','overwriter-reminder2','https://ltx1-holdemaz01.grid.linkedin.com:8443/executor?execid=5416293&job=countByCountryFlow_countByCountry&attempt=0','https://ltx1-holdemaz01.grid.linkedin.com:8443/executor?execid=5416293','https://ltx1-holdemaz01.grid.linkedin.com:8443/manager?project=AzkabanHelloPigTest&flow=countByCountryFlow&job=countByCountryFlow_countByCountry','https://elephant.linkedin.com:8443/manager?project=b2-confirm-email-reminder&flow=reminder','https://elephant.linkedin.com:8443/executor?execid=1654677&job=overwriter-reminder2&attempt=0','https://elephant.linkedin.com:8443/executor?execid=1654677','https://elephant.linkedin.com:8443/manager?project=b2-confirm-email-reminder&flow=reminder&job=overwriter-reminder2','https://elephant.linkedin.com:8443/manager?project=b2-confirm-email-reminder&flow=reminder', 200, 40, 10),
    ('application_1540411174627_3924329','\"Reportal: untitled','evlee','misc_default',1544047501675,1544061166428,'http://ltx1-farowp01.grid.linkedin.com:8080/proxy/application_1540411174627_3924329/','Hive',4,4574,0,'azkaban','untitled','https://ltx1-faroaz02.grid.linkedin.com:8443/executor?execid=1200332&job=untitled&attempt=0','https://ltx1-faroaz02.grid.linkedin.com:8443/executor?execid=1200332','https://ltx1-faroaz02.grid.linkedin.com:8443/manager?project=reportal-evlee-Huawei-Follower-Demographics&flow=data-collector&job=untitled','https://ltx1-faroaz02.grid.linkedin.com:8443/manager?project=reportal-evlee-Huawei-Follower-Demographics&flow=data-collector','https://ltx1-faroaz02.grid.linkedin.com:8443/executor?execid=1200332&job=untitled&attempt=0','https://ltx1-faroaz02.grid.linkedin.com:8443/executor?execid=1200332','https://ltx1-faroaz02.grid.linkedin.com:8443/manager?project=reportal-evlee-Huawei-Follower-Demographics&flow=data-collector&job=untitled','https://ltx1-faroaz02.grid.linkedin.com:8443/manager?project=reportal-evlee-Huawei-Follower-Demographics&flow=data-collector',1961795584,134202194,19397);

insert into yarn_app_heuristic_result(id,yarn_app_result_id,heuristic_class,heuristic_name,severity,score) values
(137594512,'application_1458194917883_1453361','com.linkedin.drelephant.mapreduce.heuristics.MapperSkewHeuristic','Mapper Skew',0,0),
(137594513,'application_1458194917883_1453361','com.linkedin.drelephant.mapreduce.heuristics.MapperGCHeuristic','Mapper GC',0,0),
(137594516,'application_1458194917883_1453361','com.linkedin.drelephant.mapreduce.heuristics.MapperTimeHeuristic','Mapper Time',3,0),
(137594520,'application_1458194917883_1453361','com.linkedin.drelephant.mapreduce.heuristics.MapperSpeedHeuristic','Mapper Speed',0,0),
(137594523,'application_1458194917883_1453361','com.linkedin.drelephant.mapreduce.heuristics.MapperSpillHeuristic','Mapper Spill',0,0),
(137594525,'application_1458194917883_1453361','com.linkedin.drelephant.mapreduce.heuristics.MapperMemoryHeuristic','Mapper Memory',3,0),
(137594530,'application_1458194917883_1453361','com.linkedin.drelephant.mapreduce.heuristics.ReducerSkewHeuristic','Reducer Skew',0,0),
(137594531,'application_1458194917883_1453361','com.linkedin.drelephant.mapreduce.heuristics.ReducerGCHeuristic','Reducer Time',0,0),
(137594534,'application_1458194917883_1453361','com.linkedin.drelephant.mapreduce.heuristics.ReducerTimeHeuristic','Reducer GC',0,0),
(137594537,'application_1458194917883_1453361','com.linkedin.drelephant.mapreduce.heuristics.ReducerMemoryHeuristic','Reducer Memory',3,0),
(137594540,'application_1458194917883_1453361','com.linkedin.drelephant.mapreduce.heuristics.ShuffleSortHeuristic','Shuffle & Sort',0,0),
(45948388,'application_1458194917883_1453361',' com.linkedin.drelephant.mapreduce.heuristics.ConfigurationHeuristic','MapReduceConfiguration',1,0),
(137594612,'application_1458194917883_1453362','com.linkedin.drelephant.mapreduce.heuristics.MapperSkewHeuristic','Mapper Skew',0,0),
(137594613,'application_1458194917883_1453362','com.linkedin.drelephant.mapreduce.heuristics.MapperGCHeuristic','Mapper GC',0,0),
(137594616,'application_1458194917883_1453362','com.linkedin.drelephant.mapreduce.heuristics.MapperTimeHeuristic','Mapper Time',0,0),
(137594620,'application_1458194917883_1453362','com.linkedin.drelephant.mapreduce.heuristics.MapperSpeedHeuristic','Mapper Speed',0,0),
(137594623,'application_1458194917883_1453362','com.linkedin.drelephant.mapreduce.heuristics.MapperSpillHeuristic','Mapper Spill',0,0),
(137594625,'application_1458194917883_1453362','com.linkedin.drelephant.mapreduce.heuristics.MapperMemoryHeuristic','Mapper Memory',3,0),
(137594630,'application_1458194917883_1453362','com.linkedin.drelephant.mapreduce.heuristics.ReducerSkewHeuristic','Reducer Skew',0,0),
(137594631,'application_1458194917883_1453362','com.linkedin.drelephant.mapreduce.heuristics.ReducerGCHeuristic','Reducer Time',3,0),
(137594634,'application_1458194917883_1453362','com.linkedin.drelephant.mapreduce.heuristics.ReducerTimeHeuristic','Reducer GC',0,0),
(137594637,'application_1458194917883_1453362','com.linkedin.drelephant.mapreduce.heuristics.ReducerMemoryHeuristic','Reducer Memory',3,0),
(137594640,'application_1458194917883_1453362','com.linkedin.drelephant.mapreduce.heuristics.ShuffleSortHeuristic','Shuffle & Sort',0,0),
(45948389,'application_1458194917883_1453362',' com.linkedin.drelephant.mapreduce.heuristics.ConfigurationHeuristic','MapReduceConfiguration',1,0);

  insert into yarn_app_heuristic_result_details (yarn_app_heuristic_result_id,name,value,details) values
(137594512,'Group A','1 tasks @ 4 MB avg','NULL'),
(137594512,'Group B','1 tasks @ 79 MB avg','NULL'),
(137594512,'Number of tasks','2','NULL'),
(137594513,'Avg task CPU time (ms)','11510','NULL'),
 (137594513,'Avg task GC time (ms)','76','NULL'),
 (137594513,'Avg task runtime (ms)','11851','NULL'),
 (137594513,'Number of tasks','2','NULL'),
 (137594513,'Task GC/CPU ratio','0.006602953953084275 ','NULL'),
 (137594516,'Average task input size','77 MB','NULL'),
 (137594516,'Average task runtime','28 sec','NULL'),
 (137594516,'Max task runtime','10 min 12 sec','NULL'),
 (137594516,'Min task runtime','13 sec','NULL'),
 (137594516,'Number of tasks','514','NULL'),
 (137594520,'Median task input size','42 MB','NULL'),
 (137594520,'Median task runtime','11 sec','NULL'),
 (137594520,'Median task speed','3 MB/s','NULL'),
 (137594520,'Number of tasks','2','NULL'),
 (137594520,'Total input size in MB','58.65','NULL'),
 (137594523,'Avg output records per task','56687','NULL'),
 (137594523,'Avg spilled records per task','79913','NULL'),
 (137594523,'Number of tasks','2','NULL'),
 (137594523,'Ratio of spilled records to output records','1.4097111356119074','NULL'),
 (137594525,'Avg Physical Memory (MB)','522','NULL'),
 (137594525,'Avg task runtime','11 sec','NULL'),
 (137594525,'Avg Virtual Memory (MB)','3307','NULL'),
 (137594525,'Max Physical Memory (MB)','595','NULL'),
 (137594525,'Max Total Committed Heap Usage Memory (MB)','427','NULL'),
 (137594525,'Max Virtual Memory (MB)','1426','NULL'),
 (137594525,'Min Physical Memory (MB)','449','NULL'),
 (137594525,'Number of tasks','2','NULL'),
 (137594525,'Requested Container Memory','2 GB','NULL'),
 (137594530,'Group A','11 tasks @ 868 KB avg','NULL'),
 (137594530,'Group B','9 tasks @ 883 KB avg ','NULL'),
 (137594530,'Number of tasks','20','NULL'),
 (137594531,'Avg task CPU time (ms)','8912','NULL'),
 (137594531,'Avg task GC time (ms)','73','NULL'),
 (137594531,'Avg task runtime (ms)','11045','NULL'),
 (137594531,'Number of tasks','20','NULL'),
 (137594531,'Task GC/CPU ratio','0.008191202872531419 ','NULL'),
 (137594534,'Average task runtime','11 sec','NULL'),
 (137594534,'Max task runtime','14 sec','NULL'),
 (137594534,'Min task runtime','8 sec','NULL'),
 (137594534,'Number of tasks','20','NULL'),
 (137594537,'Avg Physical Memory (MB)','416','NULL'),
 (137594537,'Avg task runtime','11 sec','NULL'),
 (137594537,'Avg Virtual Memory (MB)','3326','NULL'),
 (137594537,'Max Physical Memory (MB)','497','NULL'),
 (137594537,'Max Total Committed Heap Usage Memory (MB)','300','NULL'),
 (137594537,'Max Virtual Memory (MB)','1350','NULL'),
 (137594537,'Min Physical Memory (MB)','354','NULL'),
 (137594537,'Number of tasks','20','NULL'),
 (137594537,'Requested Container Memory','2 GB','NULL'),
 (137594540,'Average code runtime','1 sec','NULL'),
 (137594540,'Average shuffle time','9 sec (5.49x)','NULL'),
 (137594540,'Average sort time','(0.04x)','NULL'),
 (137594540,'Number of tasks','20','NULL'),
 (137594612,'Group A','1 tasks @ 4 MB avg','NULL'),
 (137594612,'Group B','1 tasks @ 79 MB avg','NULL'),
 (137594612,'Number of tasks','2','NULL'),
 (137594613,'Avg task CPU time (ms)','11510','NULL'),
 (137594613,'Avg task GC time (ms)','76','NULL'),
 (137594613,'Avg task runtime (ms)','11851','NULL'),
 (137594613,'Number of tasks','2','NULL'),
 (137594613,'Task GC/CPU ratio','0.006602953953084275 ','NULL'),
 (137594616,'Average task input size','42 MB','NULL'),
 (137594616,'Average task runtime','11 sec','NULL'),
 (137594616,'Max task runtime','12 sec','NULL'),
 (137594616,'Min task runtime','11 sec','NULL'),
 (137594616,'Number of tasks','2','NULL'),
 (137594620,'Median task input size','42 MB','NULL'),
 (137594620,'Median task runtime','11 sec','NULL'),
 (137594620,'Median task speed','3 MB/s','NULL'),
 (137594620,'Number of tasks','2','NULL'),
 (137594620,'Total input size in MB','58.65','NULL'),
 (137594623,'Avg output records per task','56687','NULL'),
 (137594623,'Avg spilled records per task','79913','NULL'),
 (137594623,'Number of tasks','2','NULL'),
 (137594623,'Ratio of spilled records to output records','1.4097111356119074','NULL'),
 (137594625,'Avg Physical Memory (MB)','522','NULL'),
 (137594625,'Avg task runtime','11 sec','NULL'),
 (137594625,'Avg Virtual Memory (MB)','3307','NULL'),
 (137594625,'Max Physical Memory (MB)','595','NULL'),
 (137594625,'Max Total Committed Heap Usage Memory (MB)','300','NULL'),
 (137594625,'Max Virtual Memory (MB)','2200','NULL'),
 (137594625,'Min Physical Memory (MB)','449','NULL'),
 (137594625,'Number of tasks','2','NULL'),
 (137594625,'Requested Container Memory','2 GB','NULL'),
 (137594630,'Group A','11 tasks @ 868 KB avg','NULL'),
 (137594630,'Group B','9 tasks @ 883 KB avg ','NULL'),
 (137594630,'Number of tasks','20','NULL'),
 (137594631,'Average task runtime','7 sec','NULL'),
 (137594631,'Max task runtime','10 sec','NULL'),
 (137594631,'Min task runtime','3 sec','NULL'),
 (137594631,'Number of tasks','741','NULL'),
 (137594631,'Task GC/CPU ratio','0.008191202872531419 ','NULL'),
 (137594634,'Average task runtime','11 sec','NULL'),
 (137594634,'Max task runtime','14 sec','NULL'),
 (137594634,'Min task runtime','8 sec','NULL'),
 (137594634,'Number of tasks','20','NULL'),
 (137594637,'Avg Physical Memory (MB)','416','NULL'),
 (137594637,'Avg task runtime','11 sec','NULL'),
 (137594637,'Avg Virtual Memory (MB)','3326','NULL'),
 (137594637,'Max Physical Memory (MB)','497','NULL'),
 (137594637,'Max Total Committed Heap Usage Memory (MB)','300','NULL'),
 (137594637,'Max Virtual Memory (MB)','2100','NULL'),
 (137594637,'Min Physical Memory (MB)','354','NULL'),
 (137594637,'Number of tasks','20','NULL'),
 (137594637,'Requested Container Memory','2 GB','NULL'),
 (137594640,'Average code runtime','1 sec','NULL'),
 (137594640,'Average shuffle time','9 sec (5.49x)','NULL'),
 (137594640,'Average sort time','(0.04x)','NULL'),
 (137594640,'Number of tasks','20','NULL'),
 (45948388,'Mapper Heap','-XX:ReservedCodeCacheSize=100M
-XX:MaxMetaspaceSize=256m
-XX:CompressedClassSpaceSize=256m
-XX:ParallelGCThreads=5
-Xmx1536m
-Xms512m
-Djava.net.preferIPv4Stack=true','NULL'),
(45948388,'Mapper Memory','2048','NULL'),
(45948388,'Reducer heap','-XX:ReservedCodeCacheSize=100M
-XX:MaxMetaspaceSize=256m
-XX:CompressedClassSpaceSize=256m
-XX:ParallelGCThreads=5
-Xmx1536m
-Xms512m
-Djava.net.preferIPv4Stack=true','NULL'),
(45948388,'Reducer Memory','2048','NULL'),
(45948388,'Sort Buffer','100','NULL'),
(45948388,'Sort Factor','10','NULL'),
(45948388,'Sort Spill','0.80','NULL'),
(45948388,'Split Size','9223372036854775807','NULL'),
(45948389,'Mapper Heap','-XX:ReservedCodeCacheSize=100M
-XX:MaxMetaspaceSize=256m
-XX:CompressedClassSpaceSize=256m
-XX:ParallelGCThreads=5
-Xmx1536m
-Xms512m
-Djava.net.preferIPv4Stack=true','NULL'),
(45948389,'Mapper Memory','2048','NULL'),
(45948389,'Reducer heap','-XX:ReservedCodeCacheSize=100M
-XX:MaxMetaspaceSize=256m
-XX:CompressedClassSpaceSize=256m
-XX:ParallelGCThreads=5
-Xmx1536m
-Xms512m
-Djava.net.preferIPv4Stack=true','NULL'),
(45948389,'Reducer Memory','2048','NULL'),
(45948389,'Sort Buffer','100','NULL'),
(45948389,'Sort Factor','10','NULL'),
(45948389,'Sort Spill','0.80','NULL'),
(45948389,'Split Size ','9223372036854775807','NULL');

INSERT INTO `yarn_app_heuristic_result` VALUES
(50635672,'application_1540411174627_3924329','com.linkedin.drelephant.mapreduce.heuristics.MapperSkewHeuristic','Mapper Skew',0,0),
(50635673,'application_1540411174627_3924329','com.linkedin.drelephant.mapreduce.heuristics.MapperGCHeuristic','Mapper GC',0,0),
(50635674,'application_1540411174627_3924329','com.linkedin.drelephant.mapreduce.heuristics.MapperTimeHeuristic','Mapper Time',1,0),
(50635675,'application_1540411174627_3924329','com.linkedin.drelephant.mapreduce.heuristics.MapperSpeedHeuristic','Mapper Speed',1,0),
(50635676,'application_1540411174627_3924329','com.linkedin.drelephant.mapreduce.heuristics.MapperSpillHeuristic','Mapper Spill',4,4268),
(50635677,'application_1540411174627_3924329','com.linkedin.drelephant.mapreduce.heuristics.MapperMemoryHeuristic','Mapper Memory',0,0),
(50635678,'application_1540411174627_3924329','com.linkedin.drelephant.mapreduce.heuristics.ReducerSkewHeuristic','Reducer Skew',0,0),
(50635679,'application_1540411174627_3924329','com.linkedin.drelephant.mapreduce.heuristics.ReducerGCHeuristic','Reducer GC',2,102),
(50635680,'application_1540411174627_3924329','com.linkedin.drelephant.mapreduce.heuristics.ReducerTimeHeuristic','Reducer Time',4,204),
(50635681,'application_1540411174627_3924329','com.linkedin.drelephant.mapreduce.heuristics.ReducerMemoryHeuristic','Reducer Memory',0,0),
(50635682,'application_1540411174627_3924329','com.linkedin.drelephant.mapreduce.heuristics.ShuffleSortHeuristic','Shuffle & Sort',0,0),
(50635683,'application_1540411174627_3924329','com.linkedin.drelephant.mapreduce.heuristics.ConfigurationHeuristic','MapReduceConfiguration',1,0);

--INSERT INTO `yarn_app_result` VALUES ('application_1540411174627_3924329','\"Reportal: untitled','evlee','misc_default',1544047501675,1544061166428,'http://ltx1-farowp01.grid.linkedin.com:8080/proxy/application_1540411174627_3924329/','Hive',4,4574,0,'azkaban','untitled','https://ltx1-faroaz02.grid.linkedin.com:8443/executor?execid=1200332&job=untitled&attempt=0','https://ltx1-faroaz02.grid.linkedin.com:8443/executor?execid=1200332','https://ltx1-faroaz02.grid.linkedin.com:8443/manager?project=reportal-evlee-Huawei-Follower-Demographics&flow=data-collector&job=untitled','https://ltx1-faroaz02.grid.linkedin.com:8443/manager?project=reportal-evlee-Huawei-Follower-Demographics&flow=data-collector','https://ltx1-faroaz02.grid.linkedin.com:8443/executor?execid=1200332&job=untitled&attempt=0','https://ltx1-faroaz02.grid.linkedin.com:8443/executor?execid=1200332','https://ltx1-faroaz02.grid.linkedin.com:8443/manager?project=reportal-evlee-Huawei-Follower-Demographics&flow=data-collector&job=untitled','https://ltx1-faroaz02.grid.linkedin.com:8443/manager?project=reportal-evlee-Huawei-Follower-Demographics&flow=data-collector',1961795584,134202194,19397);

INSERT INTO `yarn_app_heuristic_result_details` VALUES
(50635672,'Data skew (Group A)','221 tasks @ 56 MB avg',NULL),
(50635672,'Data skew (Group B)','846 tasks @ 97 MB avg',NULL),
(50635672,'Data skew (Number of tasks)','1067',NULL),
(50635672,'Time skew (Group A)','202 tasks @ 00:03:57 HH:MM:SS avg',NULL),
(50635672,'Time skew (Group B)','865 tasks @ 00:07:06 HH:MM:SS avg',NULL),
(50635672,'Time skew (Number of tasks)','1067',NULL),
(50635673,'Avg task CPU time (ms)','453862',NULL),
(50635673,'Avg task GC time (ms)','2008',NULL),
(50635673,'Avg task runtime (ms)','391104',NULL),
(50635673,'Number of tasks','1067',NULL),
(50635673,'Task GC/CPU ratio','0.004424252305766951',NULL),
(50635674,'Average task input size','88 MB',NULL),
(50635674,'Average task runtime','6 min 31 sec',NULL),
(50635674,'Max task runtime','15 min 11 sec',NULL),
(50635674,'Min task runtime','26 sec',NULL),
(50635674,'Number of tasks','1067',NULL),
(50635675,'Median task input size','97 MB',NULL),
(50635675,'Median task runtime','6 min 39 sec',NULL),
(50635675,'Median task speed','241 KB/s',NULL),
(50635675,'Number of tasks','1067',NULL),
(50635675,'Total input size in MB','94563.69729042053',NULL),
(50635676,'Avg output records per task','31597338',NULL),
(50635676,'Avg spilled records per task','98793173',NULL),
(50635676,'Number of tasks','1067',NULL),
(50635676,'Ratio of spilled records to output records','3.1266296189038547',NULL),
(50635677,'Avg Physical Memory (MB)','746',NULL),
(50635677,'Avg task runtime','6 min 31 sec',NULL),
(50635677,'Avg Total Committed Heap Usage Memory (MB)','688',NULL),
(50635677,'Avg Virtual Memory (MB)','2339',NULL),
(50635677,'Max Physical Memory (MB)','1044',NULL),
(50635677,'Max Total Committed Heap Usage Memory (MB)','958',NULL),
(50635677,'Max Virtual Memory (MB)','2419',NULL),
(50635677,'Min Physical Memory (MB)','460',NULL),
(50635677,'Min Total Committed Heap Usage Memory (MB)','458',NULL),
(50635677,'Min Virtual Memory (MB)','2321',NULL),
(50635677,'Number of tasks','1067',NULL),
(50635677,'Requested Container Memory','2 GB',NULL),
(50635678,'Data skew (Group A)','19 tasks @ 13 GB avg',NULL),
(50635678,'Data skew (Group B)','32 tasks @ 13 GB avg',NULL),
(50635678,'Data skew (Number of tasks)','51',NULL),
(50635678,'Time skew (Group A)','25 tasks @ 02:45:01 HH:MM:SS avg',NULL),
(50635678,'Time skew (Group B)','26 tasks @ 03:08:13 HH:MM:SS avg',NULL),
(50635678,'Time skew (Number of tasks)','51',NULL),
(50635679,'Avg task CPU time (ms)','11647985',NULL),
(50635679,'Avg task GC time (ms)','235339',NULL),
(50635679,'Avg task runtime (ms)','10610904',NULL),
(50635679,'Number of tasks','51',NULL),
(50635679,'Task GC/CPU ratio','0.020204267089973072',NULL),
(50635680,'Average task runtime','2 hr 56 min 50 sec',NULL),
(50635680,'Max task runtime','3 hr 32 min 12 sec',NULL),
(50635680,'Min task runtime','2 hr 8 min 37 sec',NULL),
(50635680,'Number of tasks','51',NULL),
(50635681,'Avg Physical Memory (MB)','1605',NULL),
(50635681,'Avg task runtime','2 hr 56 min 50 sec',NULL),
(50635681,'Avg Total Committed Heap Usage Memory (MB)','1362',NULL),
(50635681,'Avg Virtual Memory (MB)','2361',NULL),
(50635681,'Max Physical Memory (MB)','1820',NULL),
(50635681,'Max Total Committed Heap Usage Memory (MB)','1522',NULL),
(50635681,'Max Virtual Memory (MB)','2416',NULL),
(50635681,'Min Physical Memory (MB)','1437',NULL),
(50635681,'Min Total Committed Heap Usage Memory (MB)','1174',NULL),
(50635681,'Min Virtual Memory (MB)','2340',NULL),
(50635681,'Number of tasks','51',NULL),
(50635681,'Requested Container Memory','2 GB',NULL),
(50635682,'Average code runtime','2 hr 4 min 28 sec',NULL),
(50635682,'Average shuffle time','26 min 58 sec (0.22x)',NULL),
(50635682,'Average sort time','25 min 23 sec (0.20x)',NULL),
(50635682,'Number of tasks','51',NULL),
(50635683,'Mapper Heap','-XX:ReservedCodeCacheSize=100M\n-XX:MaxMetaspaceSize=256m\n-XX:CompressedClassSpaceSize=256m\n-XX:ParallelGCThreads=5\n-Xmx1536m\n-Xms512m\n-Djava.net.preferIPv4Stack=true',NULL),
(50635683,'Mapper Memory','2048',NULL),
(50635683,'Reducer heap','-XX:ReservedCodeCacheSize=100M\n-XX:MaxMetaspaceSize=256m\n-XX:CompressedClassSpaceSize=256m\n-XX:ParallelGCThreads=5\n-Xmx1536m\n-Xms512m\n-Djava.net.preferIPv4Stack=true',NULL),
(50635683,'Reducer Memory','2048',NULL),
(50635683,'Sort Buffer','100',NULL),
(50635683,'Sort Factor','10',NULL),
(50635683,'Sort Spill','0.80',NULL),
(50635683,'Split Size','536870912',NULL);