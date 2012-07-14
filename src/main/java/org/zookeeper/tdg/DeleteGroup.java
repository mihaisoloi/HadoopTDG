package org.zookeeper.tdg;

import org.apache.zookeeper.KeeperException;

import java.util.List;

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
public class DeleteGroup extends CuratorConnection {

    public void delete(String groupName) throws Exception {
        String path = "/" + groupName;

        try {
            List<String> children = client.getChildren().forPath(path);
            for (String child : children) {
                client.delete().withVersion(-1).forPath(path + "/" + child);
            }
            client.delete().withVersion(-1).forPath(path);
        } catch (KeeperException.NoNodeException e) {
            System.out.printf("Group %s does not exist\n", groupName);
            System.exit(1);
        }
    }
}