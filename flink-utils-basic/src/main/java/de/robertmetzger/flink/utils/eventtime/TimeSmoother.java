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

package de.robertmetzger.flink.utils.eventtime;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class TimeSmoother<T> extends AbstractStreamOperator<T>
    implements OneInputStreamOperator<T, T> {

  // TODO use a PriorityQueue here.
  private transient TreeMultiMap<Long, T> tree;
  private int sizeLimit = 0;


  private TimeSmoother(int sizeLimit) {
    if (sizeLimit < 0) {
      throw new IllegalArgumentException("Size limit is negative");
    }
    this.sizeLimit = sizeLimit;
  }

  public static <T> DataStream<T> forStream(DataStream<T> stream) {
    return forStream(stream, 0);
  }

  /**
   * Create time smoother.
   * @param sizeLimit The maximum number of elements to keep in the buffer of the smoother
   */
  public static <T> DataStream<T> forStream(DataStream<T> stream, int sizeLimit) {
    if (stream.getExecutionEnvironment().getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime) {
      throw new IllegalArgumentException("This operator doesn't work with processing time. "
          + "The time characteristic is set to '" + stream.getExecutionEnvironment().getStreamTimeCharacteristic() + "'.");
    }
    return stream.transform("TimeSmoother", stream.getType(), new TimeSmoother<T>(sizeLimit));
  }

  @Override
  public void open() throws Exception {
    // if tree is set, we are restored.
    if (tree == null) {
      tree = new TreeMultiMap<>();
    }
    getMetricGroup().gauge("treeSize", new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return tree.size();
      }
    });
  }

  @Override
  public void processElement(StreamRecord<T> element) throws Exception {
    if (sizeLimit > 0 && tree.size() > sizeLimit) {
      // emit lowest elements from tree
      Map.Entry<Long, List<T>> lowest = tree.firstEntry();
      for (T record: lowest.getValue()) {
        output.collect(new StreamRecord<>(record, lowest.getKey()));
      }
      tree.remove(lowest.getKey());
    }
    tree.put(element.getTimestamp(), element.getValue());
  }

  @SuppressWarnings("unchecked")
  @Override
  public void processWatermark(Watermark mark) throws Exception {
    long watermark = mark.getTimestamp();
    NavigableMap<Long, List<T>> elementsLowerOrEqualsToWatermark = tree.headMap(watermark, true);
    Iterator<Entry<Long, List<T>>> iterator = elementsLowerOrEqualsToWatermark.entrySet().iterator();
    int removed = 0;
    while (iterator.hasNext()) {
      Map.Entry<Long, List<T>> el = iterator.next();
      for (T record: el.getValue()) {
        output.collect(new StreamRecord<>(record, el.getKey()));
        removed++;
      }
      iterator.remove();
    }
    tree.reportRemoved(removed);
    output.emitWatermark(mark);
  }


  /*
  TODO enable checkpointing again.
  @Override
  public TreeMultiMap<Long, T> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
    return tree;
  }

  @Override
  public void restoreState(TreeMultiMap<Long, T> state) throws Exception {
    tree = state;
  }
  */

  public static class TreeMultiMap<K, V> implements Serializable {

    private final TreeMap<K, List<V>> tree;
    private int size;

    public TreeMultiMap() {
      this.tree = new TreeMap<>();
      this.size = 0;
    }

    public int size() {
      return size;
    }

    /**
     * Remove element by key.
     * @param key to remove
     */
    public void remove(K key) {
      // this is not the cheapest thing
      size -= tree.get(key).size();
      tree.remove(key);
    }

    /**
     * Put element into tree.
     * @param key of element
     * @param value of element
     */
    public void put(K key, V value) {
      size++;
      List<V> entry = tree.get(key);
      if (entry == null) {
        entry = new ArrayList<>();
        tree.put(key, entry);
      }
      entry.add(value);
    }

    public Map.Entry<K, List<V>> firstEntry() {
      return tree.firstEntry();
    }

    public NavigableMap<K, List<V>> headMap(K key, boolean inclusive) {
      return tree.headMap(key, inclusive);
    }

    public void reportRemoved(int removed) {
      this.size -= removed;
    }
  }
}
