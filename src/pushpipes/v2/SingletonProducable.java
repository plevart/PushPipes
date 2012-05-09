package pushpipes.v2;

/**
 * @author peter.levart@gmail.com
 */
public class SingletonProducable<T> extends SingleValuedProducable<T>
{
   private final T t;

   public SingletonProducable(T t)
   {
      this.t = t;
   }

   @Override
   public Producer producer(final SingleValuedConsumerProducer<? super T> downstream)
   {
      return new Producer.Stateful(downstream)
      {
         boolean produced;

         @Override
         protected boolean thisProduceNext()
         {
            if (produced) return false;
            produced = true;
            return downstream.consume(t);
         }
      };
   }
}
