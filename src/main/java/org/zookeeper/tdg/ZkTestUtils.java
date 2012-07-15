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
package org.zookeeper.tdg;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryOneTime;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class ZkTestUtils {

    protected static CuratorFramework client;

    public ZkTestUtils() throws IOException {
        this("localhost");
    }

    public ZkTestUtils(String host) throws IOException {
        this(CuratorFrameworkFactory.newClient(host, new RetryOneTime(1000)));
    }

    public ZkTestUtils(CuratorFramework client) {
        this.client = client;
    }

    public static CuratorFramework getClient() {
        return client;
    }

    public void createGroup(String groupName) throws Exception {
        String createdPath = client.create()
                .withMode(CreateMode.PERSISTENT)
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .forPath("/" + groupName);
        System.out.println("Created " + createdPath);
    }

    public boolean deleteGroup(String groupName) throws Exception {
        String path = "/" + groupName;

        try {
            List<String> children = client.getChildren().forPath(path);
            for (String child : children) {
                client.delete().withVersion(-1).forPath(path + "/" + child);
            }
            client.delete().withVersion(-1).forPath(path);
        } catch (KeeperException.NoNodeException e) {
            System.out.printf("Group %s does not exist\n", groupName);
            return false;
        }
        return true;
    }

    public void joinGroup(String groupName, String memberName) throws Exception {
        String createdPath = client.create().withMode(CreateMode.EPHEMERAL).forPath("/" + groupName + "/" + memberName);
        System.out.println("Created " + createdPath);
    }

    public List<String> listGroup(String groupName) throws Exception {
        List<String> children = Collections.emptyList();
        try {
            children = client.getChildren().forPath("/" + groupName);
            if (children.isEmpty()) {
                System.out.printf("No members in group %s\n", groupName);
                System.exit(1);
            }
            for (String child : children)
                System.out.println(child);
        } catch (KeeperException.NoNodeException e) {
            System.out.printf("Group %s does not exist\n", groupName);
            System.exit(1);
        }
        return children;
    }
}
