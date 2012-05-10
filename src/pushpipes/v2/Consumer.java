package pushpipes.v2;

/**
 * @author peter.levart@gmail.com
 */
public interface Consumer<T>
{
   boolean canConsume();

   void consume(T t) throws IllegalStateException;
}
