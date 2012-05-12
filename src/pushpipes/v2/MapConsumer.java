package pushpipes.v2;

/**
 * Same as {@link Consumer}, but consumes pairs of objects (key, value) via {@link #consume} method.
 *
 * @author peter.levart@gmail.com
 * @see Consumer
 * @see MapTransformer
 * @see MapProducable
 */
public interface MapConsumer<K, V>
{
   /**
    * Consumes a pair of objects (key, value).
    *
    * @param k the key to consume
    * @param v the value to consume
    */
   void consume(K k, V v);
}
