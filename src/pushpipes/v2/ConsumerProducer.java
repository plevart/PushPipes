package pushpipes.v2;

import java.util.*;

/**
 * @author peter.levart@gmail.com
 */
public interface ConsumerProducer<T> extends Consumer<T>, Producer
{
   //
   // some tail implementations

   final class Counter implements ConsumerProducer<Object>
   {
      private long count;

      @Override
      public boolean canConsume()
      {
         return true;
      }

      @Override
      public void consume(Object o)
      {
         count++;
      }

      @Override
      public boolean produce()
      {
         return false;
      }

      public long getCount()
      {
         return count;
      }
   }

   final class SingleResultTail<T> implements ConsumerProducer<T>
   {
      private boolean hasResult;
      private T result;

      public boolean hasResult()
      {
         return hasResult;
      }

      public T getResult()
      {
         return result;
      }

      //
      // ConsumerProducer

      @Override
      public boolean canConsume()
      {
         return !hasResult;
      }

      @Override
      public void consume(T t) throws IllegalStateException
      {
         if (hasResult) throw new IllegalStateException("Already has single result");
         hasResult = true;
         result = t;
      }

      @Override
      public boolean produce()
      {
         return false;
      }
   }

   final class IteratorTail<T> implements ConsumerProducer<T>, Iterator<T>
   {
      private static final Object NONE = new Object();

      @SuppressWarnings("unchecked")
      private static <T> T none() { return (T) NONE; }

      private T next = none();
      private Producer producer;

      //
      // this is injected after construction as the chain's top-most Producer

      public void setProducer(Producer producer)
      {
         this.producer = producer;
      }

      //
      // ConsumerProducer

      @Override
      public boolean canConsume()
      {
         return true;
      }

      @Override
      public void consume(T t)
      {
         next = t;
      }

      @Override
      public boolean produce()
      {
         return false;
      }

      //
      // Iterator

      @Override
      public boolean hasNext()
      {
         while (next == NONE && producer.produce()) {}
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
            next = none();
         }
      }

      @Override
      public void remove()
      {
         throw new UnsupportedOperationException();
      }
   }
}
