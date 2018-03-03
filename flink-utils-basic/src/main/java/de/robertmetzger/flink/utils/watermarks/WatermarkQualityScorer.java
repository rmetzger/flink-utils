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

package de.robertmetzger.flink.utils.watermarks;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class WatermarkQualityScorer<T> extends AbstractStreamOperator<T>
    implements OneInputStreamOperator<T, T> {

  public static <T> void score(DataStream<T> stream) {
    stream.transform("Watermark Quality Scorer", stream.getType(), new WatermarkQualityScorer<T>());
  }

  private transient long currentWatermark;
  private transient long elementsSeen;
  private transient long elementsBehindWatermark;
  private transient long lastWatermark;

  @Override
  public void open() throws Exception {
    lastWatermark = Long.MIN_VALUE;
    this.currentWatermark = Long.MIN_VALUE; // everything wil always be before the WM
    MetricGroup metricGroup = this.getMetricGroup().addGroup("watermarkQuality");
    metricGroup.gauge("currentWatermark", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return currentWatermark;
      }
    });
    metricGroup.gauge("elementsSeen", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return elementsSeen;
      }
    });
    metricGroup.gauge("elementsBehindWatermark", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return elementsBehindWatermark;
      }
    });
    metricGroup.gauge("qualityScore", new Gauge<Double>() {
      @Override
      public Double getValue() {
        return elementsBehindWatermark / (double)elementsSeen;
      }
    });
  }

  @Override
  public void processElement(StreamRecord<T> element) throws Exception {
    this.elementsSeen++;
    if (element.getTimestamp() < currentWatermark) {
      this.elementsBehindWatermark++;
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    LOG.info("Final quality score " + elementsBehindWatermark / (double)elementsSeen + " elements behind " + elementsBehindWatermark + " of " + this.elementsSeen);
    this.currentWatermark = mark.getTimestamp();
    this.elementsSeen = 0;
    this.elementsBehindWatermark = 0;

    if (this.currentWatermark < this.lastWatermark) {
      throw new IllegalStateException("Current watermark is lower than last watermark");
    }
    this.lastWatermark = currentWatermark;
  }
}
