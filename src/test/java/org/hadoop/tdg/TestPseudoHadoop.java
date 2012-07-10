package org.hadoop.tdg;

import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URL;

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
public class TestPseudoHadoop {

    private static final String DST = "hdfs://localhost/user/"+System.getProperty("user.name");
    private static final String HOME = System.getProperty("user.home");
    private static final String DST_FILE = DST + "/test";
    private static final String HOME_FILE = HOME + "/test";
    private static final Configuration CONF = new Configuration();

    static { // only once per JVM
        RandomAccessFile f = null;
        try {
            f = new RandomAccessFile(HOME + "/test", "rw");
            f.setLength(4096*1000);
        } catch (IOException e) {
            //won't happen
        } finally {
            IOUtils.closeStream(f);
        }

        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    @Test
    public void readWithURLHandler() throws IOException {
        printStream(new URL(DST_FILE).openStream());
    }

    @Test
    public void readWithFileSystem() throws IOException {
        FileSystem fs = FileSystem.get(URI.create(DST_FILE), CONF);
        FSDataInputStream is = fs.open(new Path(DST_FILE));
        is.seek(23);
        printStream(is);
    }

    @Test
    public void copyFileWithProgress() throws IOException {
        InputStream in = new BufferedInputStream(new FileInputStream(HOME_FILE));
        FileSystem fs = FileSystem.get(URI.create(DST), CONF);
        OutputStream out = fs.create(new Path(DST_FILE), new Progressable() {
            @Override
            public void progress() {
                System.out.print("~");
            }
        });

        IOUtils.copyBytes(in, out, 4096, true);
    }

    private void printStream(InputStream is) throws IOException {
        File f1 = new File(HOME_FILE);
        File f2 = new File(HOME+"/test.cpy");
        FileOutputStream fos = new FileOutputStream(f2);
        try {
            IOUtils.copyBytes(is, fos, 4096, false);
            Files.equal(f1,f2);
        } finally {
            IOUtils.closeStream(is);
        }
    }

}
