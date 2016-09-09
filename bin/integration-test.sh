#!/usr/bin/env bash
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
</property