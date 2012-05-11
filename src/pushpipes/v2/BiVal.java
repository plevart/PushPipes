package pushpipes.v2;

/**
 * @author peter.levart@gmail.com
 */
final class BiVal<K, V> implements BiValue<K, V>
{
   private final K key;
   private final V value;

   BiVal(K key, V value)
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
