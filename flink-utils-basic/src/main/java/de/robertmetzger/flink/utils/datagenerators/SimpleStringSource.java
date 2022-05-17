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

package de.robertmetzger.flink.utils.datagenerators;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.apache.commons.lang3.RandomStringUtils;

public class SimpleStringSource implements SourceFunction<String> {

  private boolean running = true;

  private long sleep = 0;

  public SimpleStringSource(long sleep) {
    this.sleep = sleep;
  }

  @Override
  public void run(SourceContext<String> ctx) throws Exception {
    while (this.running) {
      ctx.collect(RandomStringUtils.random(10));
      if (sleep > 0) {
        Thread.sleep(sleep);
      }
    }
  }

  @Override
  public void cancel() {
    running = false;
  }
}
