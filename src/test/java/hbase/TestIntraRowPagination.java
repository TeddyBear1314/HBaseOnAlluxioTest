/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package hbase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test scan/get offset and limit settings within one row through HRegion API.
 */
public class TestIntraRowPagination {

  private static HBaseUtility TEST_UTIL = new HBaseUtility();

  /**
   * Test from client side for scan with maxResultPerCF set
   *
   * @throws Exception
   */
  @Test
  public void testScanLimitAndOffset() throws Exception {
    //byte [] TABLE = HTestConst.DEFAULT_TABLE_BYTES;
    byte [][] ROWS = HTestConst.makeNAscii(HTestConst.DEFAULT_ROW_BYTES, 2);
    byte [][] FAMILIES = HTestConst.makeNAscii(HTestConst.DEFAULT_CF_BYTES, 3);
    byte [][] QUALIFIERS = HTestConst.makeNAscii(HTestConst.DEFAULT_QUALIFIER_BYTES, 10);

    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(HTestConst.DEFAULT_TABLE_BYTES));
    HRegionInfo info = new HRegionInfo(HTestConst.DEFAULT_TABLE, null, null, false);
    for (byte[] family : FAMILIES) {
      HColumnDescriptor hcd = new HColumnDescriptor(family);
      htd.addFamily(hcd);
    }
    FileSystem filesystem = FileSystem.get(TEST_UTIL.getConfiguration());
    Path rootdir = filesystem.makeQualified(
            new Path(TEST_UTIL.getConfiguration().get(HConstants.HBASE_DIR)));
    filesystem.mkdirs(rootdir);
    HRegion region =
        HRegion.createHRegion(info, rootdir, TEST_UTIL.getConfiguration(), htd);
    try {
      Put put;
      Scan scan;
      Result result;
      boolean toLog = true;

      List<Cell> kvListExp = new ArrayList<Cell>();

      int storeOffset = 1;
      int storeLimit = 3;
      for (int r = 0; r < ROWS.length; r++) {
        put = new Put(ROWS[r]);
        for (int c = 0; c < FAMILIES.length; c++) {
          for (int q = 0; q < QUALIFIERS.length; q++) {
            KeyValue kv = new KeyValue(ROWS[r], FAMILIES[c], QUALIFIERS[q], 1,
                HTestConst.DEFAULT_VALUE_BYTES);
            put.add(kv);
            if (storeOffset <= q && q < storeOffset + storeLimit) {
              kvListExp.add(kv);
            }
          }
        }
        region.put(put);
      }

      scan = new Scan();
      scan.setRowOffsetPerColumnFamily(storeOffset);
      scan.setMaxResultsPerColumnFamily(storeLimit);
      RegionScanner scanner = region.getScanner(scan);
      List<Cell> kvListScan = new ArrayList<Cell>();
      List<Cell> results = new ArrayList<Cell>();
      while (scanner.next(results) || !results.isEmpty()) {
        kvListScan.addAll(results);
        results.clear();
      }
      result = Result.create(kvListScan);
      if (kvListExp.size() == 0)
        return;

      int i = 0;
      for (Cell kv : result.rawCells()) {
        if (i >= kvListExp.size()) {
          break;  // we will check the size later
        }

        Cell kvExp = kvListExp.get(i++);
        if (toLog) {
          System.out.println("get kv is: " + kv.toString());
          System.out.println("exp kv is: " + kvExp.toString());
        }
        assertTrue("Not equal", kvExp.equals(kv));
      }

      assertEquals(kvListExp.size(), result.size());
    } finally {
      region.close();
    }
  }

}
