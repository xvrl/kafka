package org.apache.kafka.streams.kstream;

/**
 * The {@link Predicate} interface represents a predicate (boolean-valued function) of a value.
 *
 * @param <V>   value type
 */
public interface ValuePredicate<V> {

  /**
   * Test if the record with the given value satisfies the predicate.
   *
   * @param value  the value of the record
   * @return       return {@code true} if the value satisfies the predicate&mdash;{@code false} otherwise
   */
  boolean test(V value);
}
