#!/usr/bin/env bash
~/{}bin/hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.columns=HBASE_ROW_KEY,a:a1,a:a2,b:b1 -Dimporttsv.separator=, test alluxio://sr164:19998/csvfiles/
bin/hbase org.apache.hadoop.hbase.mapreduce.Export test alluxio://sr164:19998/testexport
bin/hbase org.apache.hadoop.hbase.mapreduce.Import test2 alluxio://sr164:19998/testexport
bin/hbase org.apache.hadoop.hbase.mapreduce.WALPlayer alluxio://sr164:19998/hbase/WALs test
bin/hbase org.apache.hadoop.hbase.mapreduce.RowCounter test
bin/hbase org.apache.hadoop.hbase.mapreduce.CellCounter test
