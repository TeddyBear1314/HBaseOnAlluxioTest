针对HBase-1.2.2写的测试代码,这些代码的运行的JUnitCore()今天上传
目前只上传了自己写的测试代码，hadoop Utility测试代码和集成测试代码会以脚本的方式的上传。

通过hbase shell删除hbase中所有表：disable_all '.*'
                              drop_all '.*'

第一个集成测试：(将需要hadoop的区分出来)
bin/hbase org.apache.hadoop.hbase.IntegrationTestIngestStripeCompactions
bin/hbase org.apache.hadoop.hbase.IntegrationTestIngest
hbase-1.2.2/bin/hbase org.apache.hadoop.hbase.IntegrationTestDDLMasterFailover
hbase-1.2.2/bin/hbase org.apache.hadoop.hbase.IntegrationTestsDriver -r .*\\.IntegrationTestManyRegions
hbase-1.2.2/bin/hbase org.apache.hadoop.hbase.IntegrationTestsDriver -r .*\\.IntegrationTestSendTracesTable

