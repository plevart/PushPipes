package pushpipes.v2;

import java.util.*;
import java.util.functions.*;

/**
 * @author peter.levart@gmail.com
 */
public abstract class SingleValuedProducable<T>
   implements Producable<SingleValuedConsumerProducer<? super T>>,
              Iterable<T> // YES, we are Iterable! (push-pull)
{
   //
   // chain building

   public SingleValuedProducable<T> filter(final Predicate<? super T> predicate)
   {
      return new SingleValuedProducable<T>()
      {
         @Override
         public Producer producer(final SingleValuedConsumerProducer<? super T> downstream)
         {
            return SingleValuedProducable.this.producer(
               new SingleValuedConsumerProducer.Stateless<T>(downstream)
               {
                  @Override
                  public boolean consume(T t)
                  {
                     return predicate.test(t) && downstream.consume(t);
                  }
               }
            );
         }
      };
   }

   public <U> SingleValuedProducable<U> map(final Mapper<? super T, ? extends U> mapper)
   {
      return new SingleValuedProducable<U>()
      {
         @Override
         public Producer producer(final SingleValuedConsumerProducer<? super U> downstream)
         {
            return SingleValuedProducable.this.producer(
               new SingleValuedConsumerProducer.Stateless<T>(downstream)
               {
                  @Override
                  public boolean consume(T t)
                  {
                     return downstream.consume(mapper.map(t));
                  }
               }
            );
         }
      };
   }

   public <U> SingleValuedProducable<U> flatMap(final Mapper<? super T, ? extends Iterable<U>> mapper)
   {
      return new SingleValuedProducable<U>()
      {
         @Override
         public Producer producer(final SingleValuedConsumerProducer<? super U> downstream)
         {
            return SingleValuedProducable.this.producer(
               new SingleValuedConsumerProducer.Stateful<T>(downstream)
               {
                  Iterator<U> iterator;

                  private boolean hasNext()
                  {
                     if (iterator == null) return false;
                     if (iterator.hasNext()) return true;
                     iterator = null; // early dispose
                     return false;
                  }

                  @Override
                  public boolean consume(T t)
                  {
                     if (hasNext())
                        throw new IllegalStateException("Can not consume while still producing");

                     iterator = mapper.map(t).iterator();

                     return thisProduceNext();
                  }

                  @Override
                  protected boolean thisProduceNext()
                  {
                     return hasNext() && downstream.consume(iterator.next());
                  }
               }
            );
         }
      };
   }

   //
   // execution

   public long count()
   {
      SingleValuedConsumerProducer.Counter counter = new SingleValuedConsumerProducer.Counter();
      Producer producer = producer(counter);
      while (producer.produceNext()) {}
      return counter.getCount();
   }

   /**
    * We're also Iterable
    */
   @Override
   public Iterator<T> iterator()
   {
      SingleValuedConsumerProducer.IteratorTail<T> iterator
         = new SingleValuedConsumerProducer.IteratorTail<>();

      iterator.setProducer(producer(iterator));

      return iterator;
   }
}
