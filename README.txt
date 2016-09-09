针对HBase-1.2.2写的测试代码,这些代码的运行的JUnitCore()今天上传
目前只上传了自己写的测试代码，hadoop Utility测试代码和集成测试代码会以脚本的方式的上传。

通过hbase shell删除hbase中所有表：disable_all '.*'
                              drop_all '.*'

第一个集成测试：(将需要hadoop的区分出来,每个测试记得记录时间，当测试无法通过时可以删除对应的表，让测试自己再创建一张全新的表，再不行关闭HBase，删除alluxio中数据和zookeeper中的元数据zkCli.sh
,rmr /hbase)(而且跑的时候不能有regionserver挂掉，不然一堆Timeout的错误;如果zookeeper报connection reset by peer说明并发连接数太多，在zoo.cfg增加maxClientCnxns)
hbase-1.2.2/bin/hbase org.apache.hadoop.hbase.IntegrationTestIngestStripeCompactions(20min,可能需要将checksum.verify设置为false)
hbase-1.2.2/bin/hbase org.apache.hadoop.hbase.IntegrationTestDDLMasterFailover -Dhbase.IntegrationTestDDLMasterFailover.runtime=240000 -Dhbase.IntegrationTestDDLMasterFailover.numThreads=4 -Dhbase.IntegrationTestDDLMasterFailover.numRegions=10(时间可以设置，20min)
hbase-1.2.2/bin/hbase org.apache.hadoop.hbase.mapreduce.IntegrationTestBulkLoad(这个测试代码有bug,https://issues.apache.org/jira/browse/HBASE-16558)

hbase-1.2.2/bin/hbase org.apache.hadoop.hbase.IntegrationTestIngest(20min)
hbase-1.2.2/bin/hbase org.apache.hadoop.hbase.test.IntegrationTestLoadAndVerify -Dloadmapper.num_to_write=10000 loadAndVerify(因为默认10万，机器跑不动,需要Hadoop)
hbase-1.2.2/bin/hbase org.apache.hadoop.hbase.test.IntegrationTestTimeBoundedRequestsWithRegionReplicas(20min)
hbase-1.2.2/bin/hbase org.apache.hadoop.hbase.test.IntegrationTestTimeBoundedMultiGetRequestsWithRegionReplicas(20min)
hbase-1.2.2/bin/hbase org.apache.hadoop.hbase.mapreduce.IntegrationTestImportTsv(需要mapreduce,1min)
hbase-1.2.2/bin/hbase org.apache.hadoop.hbase.IntegrationTestAcidGuarantees(1min)
hbase-1.2.2/bin/hbase org.apache.hadoop.hbase.IntegrationTestIngestWithTags(20min)

hbase-1.2.2/bin/hbase org.apache.hadoop.hbase.IntegrationTestsDriver -r .*\\.IntegrationTestManyRegions(1 min)
hbase-1.2.2/bin/hbase org.apache.hadoop.hbase.IntegrationTestsDriver -r .*\\.IntegrationTestReplication(2min30s)(需要MapReduce)
hbase-1.2.2/bin/hbase org.apache.hadoop.hbase.IntegrationTestsDriver -r .*\\.IntegrationTestRpcClient(6min)
hbase-1.2.2/bin/hbase org.apache.hadoop.hbase.IntegrationTestsDriver -r .*\\.IntegrationTestLazyCfLoading(2min)
hbase-1.2.2/bin/hbase -Dhbase.IntegrationTestIngest.runtime=600000 org.apache.hadoop.hbase.IntegrationTestsDriver -r .*\\.IntegrationTestIngestWithAC
测试IntegrationTestIngestWithACL需要在hbase-site.xml中配置：
<property>
 <name>hbase.coprocessor.region.classes</name>
 <value>org.apache.hadoop.hbase.security.access.AccessController</value>
</property>
<property>
 <name>hbase.coprocessor.master.classes</name>
 <value>org.apache.hadoop.hbase.security.access.AccessController</value>
</property>
<property>
 <name>hbase.security.access.early_out</name>
 <value>false</value>
</property>
hbase-1.2.2/bin/hbase org.apache.hadoop.hbase.IntegrationTestsDriver -r .*\\.IntegrationTestIngestWithVisibilityLabels
测试IntegrationTestIngestWithVisibilityLabels需要在hbase-site.xml中配置：
<property>
 <name>hbase.coprocessor.region.classes</name>
 <value>org.apache.hadoop.hbase.security.visibility.VisibilityController<!--,org.apache.hadoop.hbase.security.token.TokenProvider--></value>
</property>
<property>
 <name>hbase.coprocessor.master.classes</name>
 <value>org.apache.hadoop.hbase.security.visibility.VisibilityController</value>
</property>