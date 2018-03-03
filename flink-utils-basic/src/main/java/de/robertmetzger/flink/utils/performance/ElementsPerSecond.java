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

package de.robertmetzger.flink.utils.performance;

import java.io.Serializable;

/**
 * Utility for controlling the number of elements to process / emit per second.
 */
public class ElementsPerSecond implements Serializable {
  private final long eps;
  private long count;
  private long start;

  /**
   * Create a rate limiter.
   * @param eps elements per second
   */
  public ElementsPerSecond(long eps) {
    this.eps = eps;
    count = 0;
    start = System.currentTimeMillis();
  }


  /**
   * Call this method for each processed element.
   * @throws InterruptedException Happens
   */
  public void reportElement() throws InterruptedException {
    if (++count == eps) {
      long passed = (System.currentTimeMillis() - start);
      long remaining = 1000 - passed;
      if (remaining > 0) {
        Thread.sleep(remaining);
      }
      count = 0;
      start = System.currentTimeMillis();
    }
  }
}
