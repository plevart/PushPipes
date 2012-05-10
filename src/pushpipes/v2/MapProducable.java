package pushpipes.v2;

import java.util.functions.*;

/**
 * @author peter
 * @created 5/10/12 @ 4:09 PM
 */
public abstract class MapProducable<K, V> implements MapStream<K, V>
{
   /**
    * Constructs and returns a chain of:<p/>
    * head {@link Producer} -> ... intermediate ... -> downstream {@link MapConsumerProducer}<p/>
    * ...and returns the head producer.
    *
    * @param downstream a downstream {@link MapConsumerProducer} that the chain will be connected to
    * @return a head {@link Producer}
    */
   public abstract Producer producer(MapConsumerProducer<? super K, ? super V> downstream);

   //
   // building chains...

   @Override
   public MapProducable<K, V> filter(final BiPredicate<? super K, ? super V> biPredicate)
   {
      return new MapProducable<K, V>()
      {
         @Override
         public Producer producer(final MapConsumerProducer<? super K, ? super V> downstream)
         {
            return MapProducable.this.producer(
               new MapConsumerProducer<K, V>()
               {
                  @Override
                  public boolean canConsume()
                  {
                     return downstream.canConsume();
                  }

                  @Override
                  public void consume(K k, V v) throws IllegalStateException
                  {
                     if (biPredicate.eval(k, v))
                        downstream.consume(k, v);
                  }

                  @Override
                  public boolean produce()
                  {
                     return downstream.produce();
                  }
               }
            );
         }
      };
   }

   @Override
   public <W> MapProducable<K, W> map(final BiMapper<K, V, W> biMapper)
   {
      return new MapProducable<K, W>()
      {
         @Override
         public Producer producer(final MapConsumerProducer<? super K, ? super W> downstream)
         {
            return MapProducable.this.producer(
               new MapConsumerProducer<K, V>()
               {
                  @Override
                  public boolean canConsume()
                  {
                     return downstream.canConsume();
                  }

                  @Override
                  public void consume(K k, V v) throws IllegalStateException
                  {
                     downstream.consume(k, biMapper.map(k, v));
                  }

                  @Override
                  public boolean produce()
                  {
                     return downstream.produce();
                  }
               }
            );
         }
      };
   }

   @Override
   public <W> MapStream<K, W> mapValues(final Mapper<V, W> mapper)
   {
      return map(
         new BiMapper<K, V, W>()
         {
            @Override
            public W map(K k, V v)
            {
               return mapper.map(v);
            }
         }
      );
   }

   //
   // the abstract MapStream methods...

   @Override
   public boolean isEmpty()
   {
      return false;
   }

   @Override
   public Iterable<K> inputs()
   {
      return null;
   }

   @Override
   public Iterable<V> values()
   {
      return null;
   }

   @Override
   public Iterable<BiValue<K, V>> asIterable()
   {
      return null;
   }

   //
   // executing

}
