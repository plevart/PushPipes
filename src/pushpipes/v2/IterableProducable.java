package pushpipes.v2;

import java.util.*;

/**
 * @author peter
 * @created 5/9/12 @ 12:27 PM
 */
public class IterableProducable<T> extends SingleValuedProducable<T>
{
   private final Iterable<T> iterable;

   public IterableProducable(Iterable<T> iterable)
   {
      this.iterable = iterable;
   }

   @Override
   public Producer producer(final SingleValuedConsumerProducer<? super T> downstream)
   {
      return new Producer.Stateful(downstream)
      {
         final Iterator<T> iterator = iterable.iterator();


         @Override
         protected boolean thisProduceNext()
         {
            return iterator.hasNext() &&
                   (downstream.consume(iterator.next()) || iterator.hasNext());
         }
      };
   }
}
