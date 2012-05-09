package pushpipes.v2;

import java.util.*;

/**
 * @author peter
 * @created 5/9/12 @ 1:27 PM
 */
public class MapProducable<K, V> implements Producable<ProducerBiBlock<K, V>>
{
   private final Map<K, V> map;

   public MapProducable(Map<K, V> map)
   {
      this.map = map;
   }

   @Override
   public Producer producer(final ProducerBiBlock<K, V> downstream)
   {
      return new Producer()
      {
         Iterator<Map.Entry<K, V>> iterator = map.entrySet().iterator();

         @Override
         public boolean produceNext()
         {
            if (!iterator.hasNext())
               return downstream.produceNext();

            Map.Entry<K, V> next = iterator.next();
            downstream.apply(next.getKey(), next.getValue()); // can throw
            return true;
         }
      };
   }
}
