package org.hadoop.tdg;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

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
public class MaxTemperatureReducerTest {

    @Test
    public void maximumIntegerInValues() throws IOException {
        MaxTemperatureReducer reducer = new MaxTemperatureReducer();

        Text key = new Text("1950");
        Iterator<IntWritable> values = Arrays.asList(new IntWritable(10), new IntWritable(5)).iterator();
        OutputCollector<Text, IntWritable> output = mock(OutputCollector.class);

        reducer.reduce(key, values, output, null);
        verify(output).collect(key, new IntWritable(10));
    }
}
