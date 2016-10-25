package org.apache.kafka.streams.kstream;

public interface KValueStream<K, V> {

  /**
   * Create a new instance of {@link KValueStream} that consists of all elements of this stream which satisfy a predicate.
   *
   * @param predicate the instance of {@link ValuePredicate}
   * @return a {@link KValueStream} that contains only those records that satisfy the given predicate
   */
  KValueStream<K, V> filter(ValuePredicate<V> predicate);

  /**
   * Create a new instance of {@link KValueStream} that consists all elements of this stream which do not satisfy a predicate.
   *
   * @param predicate the instance of {@link ValuePredicate}
   * @return a {@link KValueStream} that contains only those records that do not satisfy the given predicate
   */
  KValueStream<K, V> filterNot(ValuePredicate<V> predicate);

  /**
   * Create a new instance of {@link KValueStream} by transforming the value of each element in this stream into a new value in the new stream.
   *
   * @param mapper        the instance of {@link ValueMapper}
   * @param <V1>          the value type of the new stream
   *
   * @return a {@link KValueStream} that contains records with unmodified keys and new values of different type
   */
  <V1> KValueStream<K, V1> map(ValueMapper<V, V1> mapper);

  /**
   * Perform an action on the value of each element of {@link KValueStream}.
   * Note that this is a terminal operation that returns void.
   *
   * @param action an action to perform on each element
   */
  void foreach(ForeachValueAction<V> action);

  /**
   * Create a new instance of {@link KValueStream} by transforming the value of each element in this stream into zero or more values with the same key in the new stream.
   *
   * @param processor     the instance of {@link ValueMapper}
   * @param <V1>          the value type of the new stream
   *
   * @return a {@link KValueStream} that contains more or less records with unmodified keys and new values of different type
   */
  <V1> KValueStream<K, V1> flatMap(ValueMapper<V, Iterable<V1>> processor);

  /**
   * Create a new {@link KValueStream} instance by applying a {@link org.apache.kafka.streams.kstream.ValueTransformer} to all values in this stream, one element at a time.
   *
   * @param valueTransformerSupplier  the instance of {@link ValueTransformerSupplier} that generates {@link org.apache.kafka.streams.kstream.ValueTransformer}
   * @param stateStoreNames           the names of the state store used by the processor
   *
   * @return a {@link KStream} that contains records with unmodified keys and transformed values with type {@code R}
   */
  <V1> KValueStream<K, V1> transform(ValueTransformerSupplier<V, V1> valueTransformerSupplier, String... stateStoreNames);

  /**
   * Creates an array of {@link KValueStream} from this stream by branching the elements in the original stream based on the supplied predicates.
   * Each element is evaluated against the supplied predicates, and predicates are evaluated in order. Each stream in the result array
   * corresponds position-wise (index) to the predicate in the supplied predicates. The branching happens on first-match: An element
   * in the original stream is assigned to the corresponding result stream for the first predicate that evaluates to true, and
   * assigned to this stream only. An element will be dropped if none of the predicates evaluate to true.
   *
   * @param predicates    the ordered list of {@link ValuePredicate} instances
   *
   * @return multiple distinct substreams of this {@link KValueStream}
   */
  KValueStream<K, V>[] branch(ValuePredicate<V>... predicates);

  KStream<K, V> entries();
}
