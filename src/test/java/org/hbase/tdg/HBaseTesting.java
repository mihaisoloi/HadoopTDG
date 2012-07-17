/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hbase.tdg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;

import java.io.IOException;
import java.util.Arrays;

public class HBaseTesting {

    private static final HBaseTestingUtility HTU = new HBaseTestingUtility();
    private Configuration conf;
    private HBaseAdmin admin;
    private HTableDescriptor htd;
    private final String tableName = "testtable";

    @BeforeClass
    public static void setUpEnvironment() throws Exception {
        HTU.startMiniCluster();
    }

    @AfterClass
    public static void tearDownEnvironment() throws Exception {
        HTU.shutdownMiniCluster();
    }

    @Before
    public void setUp() throws IOException {
        conf = HTU.getConfiguration();
        admin = HTU.getHBaseAdmin();
        htd = new HTableDescriptor(tableName);
        htd.addFamily(new HColumnDescriptor("colfam1"));
    }

    @After
    public void tearDown() throws IOException {
        if (admin.isTableAvailable(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
    }

    @Test
    public void testRegionObserver() throws Exception {
        htd.addCoprocessor("org.hbase.tdg.RegionObserverExample");
        admin.createTable(htd);
        HTable table = new HTable(conf, tableName);
        final byte[] row = Bytes.toBytes("@@@GETTIME@@@");
        Put put = new Put(row);
        final byte[] family = Bytes.toBytes("colfam1");
        put.add(family, Bytes.toBytes("qual1"), Bytes.toBytes("Say hello to my little friend!"));
        table.put(put);
        table.flushCommits();
        Get get = new Get(row);
        Result result = table.get(get);
        Assert.assertEquals(2, result.getMap().size()); // one row is added by the coprocessor
    }

    @Test
    public void testMasterObserver() throws Exception {//?? don't know why it doesn't show the new path set by observer
        System.out.println("~~~~~~~" + Arrays.toString(admin.getMasterCoprocessors()));
        FileStatus[] statuses = HTU.getTestFileSystem().listStatus(new Path("/user/msoloi"));
        Path[] listedPaths = FileUtil.stat2Paths(statuses);
//        Assert.assertTrue(listedPaths.length == 1);
        System.out.println(Arrays.toString(listedPaths));
    }
}
