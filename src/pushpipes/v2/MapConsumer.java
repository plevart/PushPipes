package pushpipes.v2;

/**
 * @author peter.levart@gmail.com
 */
public interface MapConsumer<K, V>
{
   boolean canConsume();

   void consume(K k, V v) throws IllegalStateException;
}
