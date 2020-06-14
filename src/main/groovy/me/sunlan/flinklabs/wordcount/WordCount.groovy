/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package me.sunlan.flinklabs.wordcount

import groovy.transform.CompileStatic
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

@CompileStatic
class WordCount {
    public static final long TIME_WINDOW_SECONDS = (long) (WordProvider.PROVIDE_WORD_INTERVAL / 1000) * 3

    static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
        env.disableOperatorChaining()
        env.setBufferTimeout(5)

        DataStream<String> text = env.socketTextStream("localhost", WordProvider.PORT, "\n").setParallelism(1)
        DataStream<Long> windowCounts =
                text.map(new MapFunction<String, Long>() {
                    @Override
                    Long map(String value) throws Exception {
                        Long num = Long.parseLong(value.trim())
//                        println "num: ${num}"
                        return num
                    }
                }).setParallelism(8)
        .keyBy(new KeySelector<Long, Long>() {
            @Override
            Long getKey(Long value) throws Exception {
                return value % 2
            }
        })
        .countWindow(1)
        .apply(new WindowFunction<Long, Long, Long, GlobalWindow>() {
            @Override
            void apply(Long aLong, GlobalWindow window, Iterable<Long> input, Collector<Long> out) throws Exception {
                for (n in input) {
                    out.collect(n)
                }
            }
        }).setParallelism(4)

        windowCounts.print()

        env.execute "WordCount"
    }
}
