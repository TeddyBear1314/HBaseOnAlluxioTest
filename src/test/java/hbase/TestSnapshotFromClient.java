package hbase;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.SnapshotManifestV1;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test create/using/deleting snapshots from the client
 * <p>
 * This is an end-to-end test for the snapshot utility
 */
public class TestSnapshotFromClient {
  private static final Log LOG = LogFactory.getLog(TestSnapshotFromClient.class);
  protected static final HBaseUtility UTIL = new HBaseUtility();
  private static final String STRING_TABLE_NAME = "test";
  protected static final byte[] TEST_FAM = Bytes.toBytes("fam");
  protected static final TableName TABLE_NAME =
      TableName.valueOf(STRING_TABLE_NAME);

  /**
   * Setup the config for the cluster
   * @throws Exception on failure
   */
  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
  }

  private static void setupConf(Configuration conf) {
    // disable the ui
    conf.setInt("hbase.regionsever.info.port", -1);
    // change the flush size to a small amount, regulating number of store files
    conf.setInt("hbase.hregion.memstore.flush.size", 25000);
    // so make sure we get a compaction when doing a load, but keep around some
    // files in the store
    conf.setInt("hbase.hstore.compaction.min", 10);
    conf.setInt("hbase.hstore.compactionThreshold", 10);
    // block writes if we get to 12 store files
    conf.setInt("hbase.hstore.blockingStoreFiles", 12);
    // Enable snapshot
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
      ConstantSizeRegionSplitPolicy.class.getName());
  }

  @Before
  public void setup() throws Exception {
    HTableDescriptor htd = new HTableDescriptor(TABLE_NAME);
    htd.setRegionReplication(getNumReplicas());
    UTIL.createTable(htd, new byte[][]{TEST_FAM}, UTIL.getConfiguration());
  }

  protected int getNumReplicas() {
    return 1;
  }

  @After
  public void tearDown() throws Exception {
    UTIL.deleteTableIfExists(TABLE_NAME);
    SnapshotUtility.deleteAllSnapshots(UTIL.getAdmin());
  }

  /**
   * Test snapshotting not allowed hbase:meta and -ROOT-
   * @throws Exception
   */
  @Test (timeout=300000)
  public void testMetaTablesSnapshot() throws Exception {
    Admin admin = UTIL.getAdmin();
    byte[] snapshotName = Bytes.toBytes("metaSnapshot");

    try {
      admin.snapshot(snapshotName, TableName.META_TABLE_NAME);
      fail("taking a snapshot of hbase:meta should not be allowed");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  /**
   * Test HBaseAdmin#deleteSnapshots(String) which deletes snapshots whose names match the parameter
   *
   * @throws Exception
   */
  @Test (timeout=300000)
  public void testSnapshotDeletionWithRegex() throws Exception {
    Admin admin = UTIL.getAdmin();
    // make sure we don't fail on listing snapshots
    SnapshotUtility.assertNoSnapshots(admin);

    // put some stuff in the table
    HTable table = new HTable(UTIL.getConfiguration(), TABLE_NAME);
    UTIL.loadTable(table, TEST_FAM);
    table.close();

    byte[] snapshot1 = Bytes.toBytes("TableSnapshot1");
    admin.snapshot(snapshot1, TABLE_NAME);
    LOG.debug("Snapshot1 completed.");

    byte[] snapshot2 = Bytes.toBytes("TableSnapshot2");
    admin.snapshot(snapshot2, TABLE_NAME);
    LOG.debug("Snapshot2 completed.");

    String snapshot3 = "3rdTableSnapshot";
    admin.snapshot(Bytes.toBytes(snapshot3), TABLE_NAME);
    LOG.debug(snapshot3 + " completed.");

    // delete the first two snapshots
    admin.deleteSnapshots("TableSnapshot.*");
    List<SnapshotDescription> snapshots = admin.listSnapshots();
    assertEquals(1, snapshots.size());
    assertEquals(snapshots.get(0).getName(), snapshot3);

    admin.deleteSnapshot(snapshot3);
    admin.close();
  }
  /**
   * Test snapshotting a table that is offline
   * @throws Exception
   */
  @Test (timeout=300000)
  public void testOfflineTableSnapshot() throws Exception {
    Admin admin = UTIL.getAdmin();
    // make sure we don't fail on listing snapshots
    SnapshotUtility.assertNoSnapshots(admin);

    // put some stuff in the table
    HTable table = new HTable(UTIL.getConfiguration(), TABLE_NAME);
    UTIL.loadTable(table, TEST_FAM, false);

    LOG.debug("FS state before disable:");
    FSUtils.logFileSystemState(UTIL.getTestFileSystem(),
      FSUtils.getRootDir(UTIL.getConfiguration()), LOG);
    // XXX if this is flakey, might want to consider using the async version and looping as
    // disableTable can succeed and still timeout.
    admin.disableTable(TABLE_NAME);

    LOG.debug("FS state before snapshot:");
    FSUtils.logFileSystemState(UTIL.getTestFileSystem(),
      FSUtils.getRootDir(UTIL.getConfiguration()), LOG);

    // take a snapshot of the disabled table
    final String SNAPSHOT_NAME = "offlineTableSnapshot";
    byte[] snapshot = Bytes.toBytes(SNAPSHOT_NAME);

    SnapshotDescription desc = SnapshotDescription.newBuilder()
      .setType(SnapshotDescription.Type.DISABLED)
      .setTable(STRING_TABLE_NAME)
      .setName(SNAPSHOT_NAME)
      .setVersion(SnapshotManifestV1.DESCRIPTOR_VERSION)
      .build();
    admin.snapshot(desc);
    LOG.debug("Snapshot completed.");

    // make sure we have the snapshot
    List<SnapshotDescription> snapshots = SnapshotUtility.assertOneSnapshotThatMatches(admin,
      snapshot, TABLE_NAME);


    FSUtils.logFileSystemState(UTIL.getTestFileSystem(),
      FSUtils.getRootDir(UTIL.getConfiguration()), LOG);


    admin.deleteSnapshot(snapshot);
    snapshots = admin.listSnapshots();
    SnapshotUtility.assertNoSnapshots(admin);
  }

  @Test (timeout=300000)
  public void testSnapshotFailsOnNonExistantTable() throws Exception {
    Admin admin = UTIL.getAdmin();
    // make sure we don't fail on listing snapshots
    SnapshotUtility.assertNoSnapshots(admin);
    String tableName = "_not_a_table";

    // make sure the table doesn't exist
    boolean fail = false;
    do {
    try {
      admin.getTableDescriptor(TableName.valueOf(tableName));
      fail = true;
          LOG.error("Table:" + tableName + " already exists, checking a new name");
      tableName = tableName+"!";
    } catch (TableNotFoundException e) {
      fail = false;
      }
    } while (fail);

    // snapshot the non-existant table
    try {
      admin.snapshot("fail", TableName.valueOf(tableName));
      fail("Snapshot succeeded even though there is not table.");
    } catch (SnapshotCreationException e) {
      LOG.info("Correctly failed to snapshot a non-existant table:" + e.getMessage());
    }
  }

  @Test (timeout=300000)
  public void testOfflineTableSnapshotWithEmptyRegions() throws Exception {
    // test with an empty table with one region

    Admin admin = UTIL.getAdmin();
    // make sure we don't fail on listing snapshots
    SnapshotUtility.assertNoSnapshots(admin);

    LOG.debug("FS state before disable:");
    FSUtils.logFileSystemState(UTIL.getTestFileSystem(),
      FSUtils.getRootDir(UTIL.getConfiguration()), LOG);
    admin.disableTable(TABLE_NAME);

    LOG.debug("FS state before snapshot:");
    FSUtils.logFileSystemState(UTIL.getTestFileSystem(),
      FSUtils.getRootDir(UTIL.getConfiguration()), LOG);

    // take a snapshot of the disabled table
    byte[] snapshot = Bytes.toBytes("testOfflineTableSnapshotWithEmptyRegions");
    admin.snapshot(snapshot, TABLE_NAME);
    LOG.debug("Snapshot completed.");

    // make sure we have the snapshot
    List<SnapshotDescription> snapshots = SnapshotUtility.assertOneSnapshotThatMatches(admin,
      snapshot, TABLE_NAME);


    LOG.debug("FS state after snapshot:");
    FSUtils.logFileSystemState(UTIL.getTestFileSystem(),
      FSUtils.getRootDir(UTIL.getConfiguration()), LOG);

    List<byte[]> emptyCfs = Lists.newArrayList(TEST_FAM); // no file in the region
    List<byte[]> nonEmptyCfs = Lists.newArrayList();


    admin.deleteSnapshot(snapshot);
    snapshots = admin.listSnapshots();
    SnapshotUtility.assertNoSnapshots(admin);
  }
}
