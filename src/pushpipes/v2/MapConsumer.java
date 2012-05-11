package pushpipes.v2;

/**
 * Same as {@link Consumer}, but consumes pairs of objects (key, value) via {@link #consume} method.
 *
 * @see Consumer
 * @see MapTransformer
 * @see MapProducable
 *
 * @author peter.levart@gmail.com
 */
public interface MapConsumer<K, V>
{
   /**
    * Consumes a pair of objects (key, value).
    * @param k the key to consume
    * @param v the value to consume
    * @throws IllegalStateException if this consumer is in a state that doesn't allow consuming
    */
   void consume(K k, V v) throws IllegalStateException;
}
