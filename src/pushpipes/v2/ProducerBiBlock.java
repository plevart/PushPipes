package pushpipes.v2;

import java.util.functions.*;

/**
 * @author peter
 * @created 5/9/12 @ 12:57 PM
 */
public abstract class ProducerBiBlock<K, V> implements Producer, BiBlock<K, V>
{
   public static <K, V> ProducerBiBlock<K, V> tail(final BiBlock<K, V> biBlock)
   {
      return new Tail<K, V>()
      {
         @Override
         public void apply(K k, V v)
         {
            biBlock.apply(k, v);
         }
      };
   }

   public static abstract class Tail<K, V> extends ProducerBiBlock<K, V>
   {
      @Override
      public boolean produceNext()
      {
         return false;
      }
   }
}
