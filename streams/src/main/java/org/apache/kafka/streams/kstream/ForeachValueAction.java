/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.kstream;

/**
 * The {@link ForeachValueAction} interface for performing an action on a value.
 * Note that this action is stateless. If stateful processing is required, consider
 * using {@link KStream#transform(TransformerSupplier, String...)} or
 * {@link KStream#process(ProcessorSupplier, String...)} instead.
 *
 * @param <V>   original value type
 */
public interface ForeachValueAction<V> {

  /**
   * Perform an action for each record of a stream.
   *
   * @param value  the value of the record
   */
  void apply(V value);
}

