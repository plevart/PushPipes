package pushpipes.v2;

/**
 * Base contract for consumers - objects that consume input passed to them via single-argument method
 * {@link #consume(Object)}.
 *
 * @see Producer
 * @see Transformer
 * @see Producable
 *
 * @author peter.levart@gmail.com
 */
public interface Consumer<T>
{
   /**
    * Consumes one object.
    * @param t the object to consume
    * @throws IllegalStateException if this consumer is in a state that doesn't allow consuming
    */
   void consume(T t) throws IllegalStateException;
}
