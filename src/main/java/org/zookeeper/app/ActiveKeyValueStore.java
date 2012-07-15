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
package org.zookeeper.app;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.zookeeper.tdg.ZkTestUtils;

public class ActiveKeyValueStore extends ZkTestUtils{

    public static final String CHARSET = "UTF-8";

    public ActiveKeyValueStore(CuratorFramework client) {
        super(client);
    }

    public void write(String path, String value) throws Exception {
        Stat stat = client.checkExists().forPath(path);
        if (stat == null)
            client.create()
                    .withMode(CreateMode.PERSISTENT)
                    .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                    .forPath(path, value.getBytes(CHARSET));
        else
            client.setData().forPath(path, value.getBytes(CHARSET));
    }

    public String read(String path, CuratorWatcher watcher) throws Exception {
        byte[] data = client.getData().usingWatcher(watcher).forPath(path);
        return new String(data,CHARSET);
    }
}
