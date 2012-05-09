package pushpipes.v2;

import java.util.*;

/**
 * @author peter
 * @created 5/9/12 @ 4:20 PM
 */
public interface SingleValuedConsumerProducer<T> extends Producer
{
   /**
    * Consume parameter t
    *
    * @param t a parameter to consume
    * @return true if this or any of the downstream producers has data buffered and
    *         next calls should be {@link #produceNext} until they return false.
    */
   boolean consume(T t);

   abstract class Stateless<T> extends Producer.Stateless implements SingleValuedConsumerProducer<T>
   {
      protected Stateless(Producer downstream)
      {
         super(downstream);
      }
   }

   abstract class Stateful<T> extends Producer.Stateful implements SingleValuedConsumerProducer<T>
   {
      protected Stateful(Producer downstream)
      {
         super(downstream);
      }
   }

   abstract class Tail<T> extends Producer.Tail implements SingleValuedConsumerProducer<T>
   {
      protected abstract void consumeImpl(T t);

      @Override
      public final boolean consume(T t)
      {
         consumeImpl(t);
         return false;
      }
   }

   final class Counter extends Tail<Object>
   {
      private long count;

      @Override
      protected void consumeImpl(Object o)
      {
         count++;
      }

      public long getCount()
      {
         return count;
      }
   }

   final class IteratorTail<T> extends Tail<T> implements Iterator<T>
   {
      private static final Object NONE = new Object();

      private T next = (T) NONE;
      private Producer producer;

      public void setProducer(Producer producer)
      {
         this.producer = producer;
      }

      @Override
      public boolean hasNext()
      {
         while (next == NONE && producer.produceNext()) {}
         return next != NONE;
      }

      @Override
      public T next()
      {
         if (!hasNext())
            throw new NoSuchElementException();

         try
         {
            return next;
         }
         finally
         {
            next = (T) NONE;
         }
      }

      @Override
      protected void consumeImpl(T t)
      {
         next = t;
      }

      @Override
      public void remove()
      {
         throw new UnsupportedOperationException();
      }
   }
}
