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
package org.hadoop.tdg;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class MaxTemperatureMapperTest {

    @Test
    public void processValidRecord() throws IOException {
        MaxTemperatureMapper mapper = new MaxTemperatureMapper();

        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                      // Year ^^^^
                "99999V0203201N00261220001CN9999999N9-00111+99999999999");
                                      // Temperature ^^^^^
        OutputCollector<Text,IntWritable> output = mock(OutputCollector.class);
        mapper.map(null,value,output,null);
        verify(output).collect(new Text("1950"), new IntWritable(-11));
    }

    @Test
    public void ignoresMissingTemperatureRecord() throws IOException {
        MaxTemperatureMapper mapper = new MaxTemperatureMapper();
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                      // Year ^^^^
                "99999V0203201N00261220001CN9999999N9+99991+99999999999");
                                      // Temperature ^^^^^
        OutputCollector<Text, IntWritable> output = mock(OutputCollector.class);
        mapper.map(null, value, output, null);
        verify(output, never()).collect(any(Text.class), any(IntWritable.class));
    }

}
