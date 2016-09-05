/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static junit.framework.TestCase.assertEquals;

/**
 * This test sets the multi size WAAAAAY low and then checks to make sure that gets will still make
 * progress.
 */
public class TestMultiRespectsLimits {
  private final static HBaseUtility TEST_UTIL = new HBaseUtility();
  private final static byte[] FAMILY = Bytes.toBytes("D");
  public static final int MAX_SIZE = 500;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setLong(
        HConstants.HBASE_SERVER_SCANNER_MAX_RESULT_SIZE_KEY,
        MAX_SIZE);
  }

  @Test
  public void testMultiLimits() throws Exception {
    final TableName name = TableName.valueOf("testMultiLimits");
    Table t = TEST_UTIL.createTable(name, FAMILY);
    TEST_UTIL.loadTable(t, FAMILY, false);

    // Split the table to make sure that the chunking happens accross regions.
    try (final Admin admin = TEST_UTIL.getAdmin()) {
      admin.split(name);
      TEST_UTIL.waitFor(60000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return admin.getTableRegions(name).size() > 1;
        }
      });
    }
    List<Get> gets = new ArrayList<>(MAX_SIZE);

    for (int i = 0; i < MAX_SIZE; i++) {
      gets.add(new Get(HBaseUtility.ROWS[i]));
    }


    Result[] results = t.get(gets);
    assertEquals(MAX_SIZE, results.length);


  }

  @Test
  public void testBlockMultiLimits() throws Exception {
    final TableName name = TableName.valueOf("testBlockMultiLimits");
    HTableDescriptor desc = new HTableDescriptor(name);
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
    hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
    desc.addFamily(hcd);
    TEST_UTIL.getAdmin().createTable(desc);
    Table t = TEST_UTIL.getConnection().getTable(name);


    byte[] row = Bytes.toBytes("TEST");
    byte[][] cols = new byte[][]{
        Bytes.toBytes("0"), // Get this
        Bytes.toBytes("1"), // Buffer
        Bytes.toBytes("2"), // Buffer
        Bytes.toBytes("3"), // Get This
        Bytes.toBytes("4"), // Buffer
        Bytes.toBytes("5"), // Buffer
    };

    // Set the value size so that one result will be less than the MAX_SIE
    // however the block being reference will be larger than MAX_SIZE.
    // This should cause the regionserver to try and send a result immediately.
    byte[] value = new byte[MAX_SIZE - 100];
    ThreadLocalRandom.current().nextBytes(value);

    for (byte[] col:cols) {
      Put p = new Put(row);
      p.addImmutable(FAMILY, col, value);
      t.put(p);
    }

    // Make sure that a flush happens
    try (final Admin admin = TEST_UTIL.getAdmin()) {
      admin.flush(name);
      TEST_UTIL.waitFor(60000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return true;
        }
      });
    }

    List<Get> gets = new ArrayList<>(2);
    Get g0 = new Get(row);
    g0.addColumn(FAMILY, cols[0]);
    gets.add(g0);

    Get g2 = new Get(row);
    g2.addColumn(FAMILY, cols[3]);
    gets.add(g2);

    Result[] results = t.get(gets);
    assertEquals(2, results.length);

  }
}
