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

import java.util.concurrent.atomic.AtomicLong

@CompileStatic
class WordProvider {
    public static final int PORT = 9090
    public static final int PROVIDE_WORD_INTERVAL = 2000
    public static final AtomicLong SEQ  = new AtomicLong(0)

    static void main(String[] args) {
        def socketServer = new ServerSocket(PORT)

        println "starting WordProvider..."
        Thread.start {
            socketServer.accept { socket ->
                socket.withStreams { input, output ->
                    while (true) {
//                        def word = "hello${new Random().nextInt(3)}"
//                        println "<<<<<<<<<<<<   ${word}"
                        output << "${SEQ.getAndIncrement()}\n"
//                        Thread.sleep(PROVIDE_WORD_INTERVAL)
                    }
                }
            }
        }
        println "WordProvider started!"
    }
}
