package pushpipes.v2;

/**
 * @author peter
 * @created 5/9/12 @ 1:23 PM
 */
public class ArrayProducable<T> extends SingleValuedProducable<T>
{
   private final T[] elements;

   @SafeVarargs
   public ArrayProducable(T... elements)
   {
      this.elements = elements;
   }

   @Override
   public Producer producer(final SingleValuedConsumerProducer<? super T> downstream)
   {
      return new Producer.Stateful(downstream)
      {
         int i;

         @Override
         protected boolean thisProduceNext()
         {
            return i < elements.length &&
                   (downstream.consume(elements[i++]) || i < elements.length);
         }
      };
   }
}
