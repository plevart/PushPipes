package pushpipes.v2;

import java.util.NoSuchElementException;
import java.util.functions.BiBlock;
import java.util.functions.BiPredicate;

/**
 * Same as {@link Transformer} but instead of being a {@link Consumer} it is a {@link MapConsumer}.
 *
 * @author peter.levart@gmail.com
 * @see Transformer
 * @see MapConsumer
 * @see Producer
 * @see MapProducable
 */
public interface MapTransformer<K, V> extends MapConsumer<K, V>, Producer
{
   /**
    * @return true if this transformer is in a state that allows consuming input so that the
    *         next call to {@link #consume} will not throw {@link IllegalStateException}.
    */
   boolean canConsume();

   /**
    * @param k the key to consume
    * @param v the value to consume
    * @throws IllegalStateException if this transformer is in a state that doesn't allow consuming
    */
   @Override
   void consume(K k, V v) throws IllegalStateException;

   //
   // some tail implementations

   abstract class Tail<K, V> implements MapTransformer<K, V>
   {
      @Override
      public final boolean canConsume()
      {
         return true;
      }

      @Override
      public final boolean produce()
      {
         return false;
      }
   }

   final class MapConsumerTail<K, V> extends Tail<K, V>
   {
      private final MapConsumer<K, V> mapConsumer;

      public MapConsumerTail(MapConsumer<K, V> mapConsumer)
      {
         this.mapConsumer = mapConsumer;
      }

      @Override
      public void consume(K k, V v) throws IllegalStateException
      {
         mapConsumer.consume(k, v);
      }
   }

   final class ResultCounterTail extends Tail<Object, Object>
   {
      private long count;

      @Override
      public void consume(Object k, Object v) throws IllegalStateException
      {
         count++;
      }

      public long getResultCount(Producer producer)
      {
         while (producer.produce()) {}
         return count;
      }
   }

   final class BiBlockTail<K, V> extends Tail<K, V>
   {
      private final BiBlock<? super K, ? super V> biBlock;

      public BiBlockTail(BiBlock<? super K, ? super V> biBlock)
      {
         this.biBlock = biBlock;
      }

      @Override
      public void consume(K k, V v) throws IllegalStateException
      {
         biBlock.apply(k, v);
      }
   }

   final class SingleResultTail<K, V> extends Tail<K, V>
   {
      private K resultKey;
      private V resultValue;
      private boolean hasResult;
      private Producer producer;

      //
      // this is injected after construction as the chain's top-most Producer

      void setProducer(Producer producer)
      {
         this.producer = producer;
      }

      //
      // Consumer

      @Override
      public void consume(K k, V v)
      {
         if (hasResult)
            throw new IllegalStateException("Multiple results");

         resultKey = k;
         resultValue = v;
         hasResult = true;
      }

      //
      // single/first result retrieval

      public boolean hasResult()
      {
         while (!hasResult && producer.produce()) {}
         return hasResult;
      }

      public BiValue<K, V> getFirstResult() throws NoSuchElementException
      {
         if (hasResult())
            return new BiVal<>(resultKey, resultValue);
         else
            throw new NoSuchElementException();
      }

      public BiValue<K, V> getOnlyResult() throws NoSuchElementException, IllegalStateException
      {
         try
         {
            return getFirstResult();
         }
         finally
         {
            // this will throw IllegalStateException if there is a second result...
            while (producer.produce()) {}
         }
      }
   }

   final class FirstMatchTail<K, V> extends Tail<K, V>
   {
      private final BiPredicate<? super K, ? super V> predicate;
      private boolean match;

      public FirstMatchTail(BiPredicate<? super K, ? super V> predicate)
      {
         this.predicate = predicate;
      }

      @Override
      public void consume(K k, V v) throws IllegalStateException
      {
         match = predicate.eval(k, v);
      }

      public boolean isMatch(Producer producer)
      {
         while (!match && producer.produce()) {}
         return match;
      }
   }
}
