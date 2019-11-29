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
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

@CompileStatic
class WordCount {
    public static final long TIME_WINDOW_SECONDS = (long) (WordProvider.PROVIDE_WORD_PERIOD / 1000) * 3

    static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
        DataStream<String> text = env.socketTextStream("localhost", WordProvider.PORT, "\n")
        DataStream<Tuple2<String, Integer>> windowCounts = text.flatMap(
                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                        value.split("\\s").each { String word ->
                            out.collect(Tuple2.of(word, 1))
                        }
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(TIME_WINDOW_SECONDS))
                .sum(1)
//                .reduce { t1, t2 -> new Tuple2<>(t1.f0, t1.f1 + t2.f1) }

        DataStreamSink<Tuple2<String, Integer>> print = windowCounts.print()
        print.setParallelism(1)

        env.execute "WordCount"
    }
}
