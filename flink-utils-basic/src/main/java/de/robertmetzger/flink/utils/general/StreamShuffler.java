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

package de.robertmetzger.flink.utils.general;

import java.util.Random;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.flink.util.XORShiftRandom;

/**
 * Shuffle elements in a stream.
 * Note: This operator does not take watermarks into account.
 *
 * @param <T> Type in the stream
 */
public class StreamShuffler<T> extends RichFlatMapFunction<T, T> {
  private final int windowSize;
  protected final Random random = new XORShiftRandom();
  private transient T[] buffer;
  private transient int fillIndex;

  public StreamShuffler(int windowSize) {
    this.windowSize = windowSize;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    this.buffer = (T[]) new Object[windowSize];
    this.fillIndex = 0;
  }

  @Override
  public void flatMap(T value, Collector<T> out) throws Exception {
    if (fillIndex < buffer.length) {
      // fill buffer
      buffer[fillIndex++] = value;
    } else {
      // buffer full. Randomly select element for emission.
      int replaceIndex = getNext(buffer.length);
      out.collect(buffer[replaceIndex]);
      buffer[replaceIndex] = value;
    }
  }

  public int getNext(int bound) {
    return random.nextInt(bound);
  }
}

