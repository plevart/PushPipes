package pushpipes.v2;

/**
 * Base contract for consumers - objects that consume input passed to them via single-argument method
 * {@link #consume(Object)}.
 *
 * @author peter.levart@gmail.com
 * @see Producer
 * @see Transformer
 * @see Producable
 */
public interface Consumer<T>
{
   /**
    * Consumes one object.
    *
    * @param t the object to consume
    */
   void consume(T t);
}
