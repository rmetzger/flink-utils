package de.robertmetzger.flink.utils.performance;

/*
 *    Copyright 2017 Robert Metzger and contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ThroughputLogger<T> implements FlatMapFunction<T, Integer> {

    private static final Logger LOG = LoggerFactory.getLogger(ThroughputLogger.class);

    private long totalReceived = 0;
    private long lastTotalReceived = 0;
    private long lastLogTimeMs = -1;
    private long logfreq;

    public ThroughputLogger(long logfreq) {
        this.logfreq = logfreq;
    }

    @Override
    public void flatMap(T element, Collector<Integer> collector) throws Exception {
        totalReceived++;
        if (totalReceived % logfreq == 0) {
            // throughput over entire time
            long now = System.currentTimeMillis();

            // throughput for the last "logfreq" elements
            if(lastLogTimeMs == -1) {
                // init (the first)
                lastLogTimeMs = now;
                lastTotalReceived = totalReceived;
            } else {
                long timeDiff = now - lastLogTimeMs;
                long elementDiff = totalReceived - lastTotalReceived;
                double ex = (1000/(double)timeDiff);
                LOG.info("During the last {} ms, we received {} elements. That's {} elements/second/core. {} MB/sec/core. GB received {}",
                        timeDiff, elementDiff, elementDiff*ex, elementDiff*ex*15 / 1024 / 1024, (totalReceived * 15) / 1024 / 1024 / 1024);
                // reinit
                lastLogTimeMs = now;
                lastTotalReceived = totalReceived;
            }
        }
    }
}