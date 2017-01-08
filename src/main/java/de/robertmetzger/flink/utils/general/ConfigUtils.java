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

import org.apache.flink.configuration.Configuration;

public class ConfigUtils {

  /**
   * You need the "flink-metrics-jmx" dependency to use this utility.
   *
   * @param configuration configuration to add the reporter to.
   */
  public static void enableJmx(Configuration configuration) {
    configuration.setString("metrics.reporters", "my_jmx_reporter");
    configuration.setString("metrics.reporter.my_jmx_reporter.class", "org.apache.flink.metrics.jmx.JMXReporter");
    configuration.setString("metrics.reporter.my_jmx_reporter.port", "9020-9040");
  }

  // TODO add enableWebServer() utility

}
