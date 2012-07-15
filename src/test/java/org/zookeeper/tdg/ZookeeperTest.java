/**
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
package org.zookeeper.tdg;

import com.google.common.base.Preconditions;
import com.netflix.curator.test.TestingServer;
import org.apache.tools.ant.types.Assertions;
import org.junit.*;
import org.zookeeper.app.ConfigUpdater;
import org.zookeeper.app.ConfigWatcher;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class ZookeeperTest {
    public static final int TIMEOUT = 1000;
    public static final String PATH = "zoo", CONNECTION = "localhost";
    public static ZkTestUtils utils;
    public static TestingServer server;

    @BeforeClass
    public static void setUpEnvironment() throws Exception {
        server = new TestingServer(2181);
        try {
            utils =  new ZkTestUtils();
            utils.getClient().start();
        } catch (IOException e) {
            //do nothing
        }
    }

    @AfterClass
    public static void tearDownEnvironment() throws IOException {
        utils.getClient().close();
        server.close();
    }

    @Test
    public void testGroups() throws Exception {
        utils.createGroup(PATH);
        new Thread(new Runnable() {
            @Override
            public void run() {

                try {
                    utils.joinGroup(PATH, "mihai");
                    Thread.sleep(TIMEOUT);
                } catch (Exception e) {
                    //do nothing
                }
            }
        }).start();
        assertNotSame(Collections.emptyList(), utils.listGroup(PATH));
        assertTrue(utils.deleteGroup(PATH));
    }

    @Test
    public void testConfig() throws Exception {
        Thread t = new Thread(new ConfigUpdater(utils.getClient()));
        t.start();

        Thread.sleep(TIMEOUT);

        ConfigWatcher configWatcher = new ConfigWatcher(utils.getClient());
        configWatcher.displayConfig();

        while (t.isAlive())
            Thread.sleep(TIMEOUT);

        assertEquals(3,configWatcher.readNumber);
    }
}
