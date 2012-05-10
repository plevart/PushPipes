package pushpipes.v2;

import java.util.*;

/**
 * @author peter.levart@gmail.com
 */
public interface MapConsumerProducer<K, V> extends MapConsumer<K, V>, Producer
{
   //
   // some tail implementations

   final class Counter implements MapConsumerProducer<Object, Object>
   {
      private long count;

      @Override
      public boolean canConsume()
      {
         return true;
      }

      @Override
      public void consume(Object o, Object o1) throws IllegalStateException
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

   final class IteratorTail<K, V> implements MapConsumerProducer<K, V>, Iterator<BiValue<K, V>>
   {
      private static final Object NONE = new Object();

      @SuppressWarnings("unchecked")
      private static <T> T none() { return (T) NONE; }

      private K nextKey = none();
      private V nextValue = none();
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
      public void consume(K k, V v) throws IllegalStateException
      {
         nextKey = k;
         nextValue = v;
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
         while (nextKey == NONE && producer.produce()) {}
         return nextKey != NONE;
      }

      @Override
      public BiValue<K, V> next()
      {
         if (!hasNext())
            throw new NoSuchElementException();

         try
         {
            return new BiVal<>(nextKey, nextValue);
         }
         finally
         {
            nextKey = none();
            nextValue = none();
         }
      }

      private static final class BiVal<K, V> implements BiValue<K, V>
      {
         private final K key;
         private final V value;

         private BiVal(K key, V value)
         {
            this.key = key;
            this.value = value;
         }

         @Override
         public K getKey()
         {
            return key;
         }

         @Override
         public V getValue()
         {
            return value;
         }
      }

      @Override
      public void remove()
      {
         throw new UnsupportedOperationException();
      }
   }
}
