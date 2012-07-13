package org.zookeeper.app;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.zookeeper.tdg.ConnectionWatcher;

import java.io.UnsupportedEncodingException;

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
public class ActiveKeyValueStore extends ConnectionWatcher {

    public static final String CHARSET = "UTF-8";

    public void write(String path, String value) throws InterruptedException, KeeperException, UnsupportedEncodingException {
        Stat stat = zk.exists(path, false);
        if (stat == null)
            zk.create(path, value.getBytes(CHARSET), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        else
            zk.setData(path, value.getBytes(CHARSET), -1);
    }

    public String read(String path, Watcher watcher) throws InterruptedException, KeeperException, UnsupportedEncodingException {
        byte[] data = zk.getData(path,watcher,null);
        return new String(data,CHARSET);
    }
}
