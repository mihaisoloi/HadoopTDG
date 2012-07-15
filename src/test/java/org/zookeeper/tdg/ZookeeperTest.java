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

import com.netflix.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zookeeper.app.ConfigUpdater;
import org.zookeeper.app.ConfigWatcher;

public class ZookeeperTest {
    public static final int TIMEOUT = 1000;
    public static final String PATH = "zoo", CONNECTION = "localhost";

    @BeforeClass
    public static void setUpEnvironment() throws Exception {
        new TestingServer(2181);
    }

    @Before
    public void setUp() throws Exception {
        CreateGroup createGroup = new CreateGroup();
        createGroup.connect(CONNECTION);
        createGroup.create(PATH);
        createGroup.close();
    }

    @After
    public void tearDown() throws Exception {
        DeleteGroup deleteGroup = new DeleteGroup();
        deleteGroup.connect(CONNECTION);
        deleteGroup.delete(PATH);
        deleteGroup.close();
    }

    @Test
    public void testListGroup() throws Exception {
        new Thread(new Runnable() {
            @Override
            public void run() {

                try {
                    JoinGroup joinGroup = new JoinGroup();
                    joinGroup.connect(CONNECTION);
                    joinGroup.join(PATH, "mihai");
                    Thread.sleep(TIMEOUT);
                } catch (Exception e) {
                    //do nothing
                }
            }
        }).start();
        ListGroup listGroup = new ListGroup();
        listGroup.connect(CONNECTION);
        listGroup.list(PATH);
        listGroup.close();
    }

    @Test
    public void testConfig() throws Exception {
        Thread t = new Thread(new ConfigUpdater(CONNECTION));
        t.start();

        Thread.sleep(TIMEOUT);

        ConfigWatcher configWatcher = new ConfigWatcher(CONNECTION);
        configWatcher.displayConfig();

        while (t.isAlive())
            Thread.sleep(TIMEOUT);
    }
}
