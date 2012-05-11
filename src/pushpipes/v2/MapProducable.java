package pushpipes.v2;

import java.util.*;
import java.util.functions.*;

/**
 * Same as {@link Producable} but establishes a chain with {@link MapTransformer} or {@link MapConsumer} at
 * it's end instead of {@link Transformer} or {@link Consumer}.
 *
 * @author peter.levart@gmail.com
 * @see Producer
 * @see MapTransformer
 * @see MapConsumer
 */
public abstract class MapProducable<K, V> implements MapStream<K, V>
{
   /**
    * Constructs and returns a chain of:<p/>
    * head {@link Producer} -> ... (Map)Transformers ... -> downstream {@link MapTransformer}<p/>
    * ...and returns the head producer.
    *
    * @param downstream a downstream {@link MapTransformer} that the chain will be connected to
    * @return a head {@link Producer}
    */
   public abstract Producer producer(MapTransformer<? super K, ? super V> downstream);

   /**
    * Constructs and returns a chain of:
    * <pre>
    * head {@link Producer} -> ... (Map)Transformers ... -> {@link MapConsumer}
    * </pre>
    * ...and returns the head producer.
    *
    * @param mapConsumer a {@link MapConsumer} that the chain will be connected to
    * @return a head {@link Producer}
    */
   public Producer producer(MapConsumer<? super K, ? super V> mapConsumer)
   {
      if (mapConsumer instanceof MapTransformer<?, ?>)
         return producer((MapTransformer<? super K, ? super V>) mapConsumer);
      else
         return producer(new MapTransformer.MapConsumerTail<>(mapConsumer));
   }

   //
   // building chains

   @Override
   public MapProducable<K, V> filter(final BiPredicate<? super K, ? super V> biPredicate)
   {
      return new MapProducable<K, V>()
      {
         @Override
         public Producer producer(final MapTransformer<? super K, ? super V> downstream)
         {
            return MapProducable.this.producer(
               new MapTransformer<K, V>()
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
   public MapProducable<K, V> filterKeys(final Predicate<K> filter)
   {
      return filter(
         new BiPredicate<K, V>()
         {
            @Override
            public boolean eval(K k, V v)
            {
               return filter.test(k);
            }
         }
      );
   }

   @Override
   public MapProducable<K, V> filterValues(final Predicate<V> filter)
   {
      return filter(
         new BiPredicate<K, V>()
         {
            @Override
            public boolean eval(K k, V v)
            {
               return filter.test(v);
            }
         }
      );
   }

   @Override
   public <W> MapProducable<K, W> map(final BiMapper<K, V, W> biMapper)
   {
      return new MapProducable<K, W>()
      {
         @Override
         public Producer producer(final MapTransformer<? super K, ? super W> downstream)
         {
            return MapProducable.this.producer(
               new MapTransformer<K, V>()
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
   public <W> MapProducable<K, W> mapValues(final Mapper<V, W> mapper)
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

   @Override
   public MapProducable<V, K> swap()
   {
      return new MapProducable<V, K>()
      {
         @Override
         public Producer producer(final MapTransformer<? super V, ? super K> downstream)
         {
            return MapProducable.this.producer(
               new MapTransformer<K, V>()
               {
                  @Override
                  public boolean canConsume()
                  {
                     return downstream.canConsume();
                  }

                  @Override
                  public void consume(K k, V v) throws IllegalStateException
                  {
                     downstream.consume(v, k);
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
   public MapProducable<K, V> sorted(final Comparator<? super K> comparator)
   {
      return new MapProducable<K, V>()
      {
         @Override
         public Producer producer(final MapTransformer<? super K, ? super V> downstream)
         {
            return MapProducable.this.producer(
               new MapTransformer<K, V>()
               {
                  @SuppressWarnings("unchecked")
                  BiValue<K, V>[] array = (BiValue<K, V>[]) new Object[Producable.DEFAULT_BUFFER_CAPACITY];
                  int size;
                  boolean sorted;

                  @Override
                  public boolean canConsume()
                  {
                     return !sorted;
                  }

                  @Override
                  public void consume(K k, V v) throws IllegalStateException
                  {
                     if (!canConsume())
                        throw new IllegalStateException("Can not consume after already sorting and producing output");

                     if (size >= array.length)
                        array = Arrays.copyOf(array, array.length << 1);

                     array[size++] = new BiVal<>(k, v);
                  }

                  int i;

                  @Override
                  public boolean produce()
                  {
                     if (!sorted)
                     {
                        Arrays.sort(array, 0, size, new Comparator<BiValue<K, V>>()
                        {
                           @Override
                           public int compare(BiValue<K, V> bv1, BiValue<K, V> bv2)
                           {
                              return comparator.compare(bv1.getKey(), bv2.getKey());
                           }
                        });

                        sorted = true;
                     }

                     if (i < size && downstream.canConsume())
                     {
                        downstream.consume(array[i].getKey(), array[i++].getValue());
                        return true;
                     }

                     return downstream.produce();
                  }
               }
            );
         }
      };
   }

   @Override
   public MapProducable<K, V> merge(final MapStream<K, V> other)
   {
      if (other instanceof MapProducable<?, ?>)
      {
         return merge((MapProducable<K, V>) other);
      }
      else
      {
         return new MapProducable<K, V>()
         {
            @Override
            public Producer producer(final MapTransformer<? super K, ? super V> downstream)
            {
               return MapProducable.this.producer(
                  new MapTransformer<K, V>()
                  {
                     Iterator<BiValue<K, V>> otherIterator = other.asIterable().iterator();

                     @Override
                     public boolean canConsume()
                     {
                        return downstream.canConsume();
                     }

                     @Override
                     public void consume(K k, V v) throws IllegalStateException
                     {
                        downstream.consume(k, v);
                     }

                     @Override
                     public boolean produce()
                     {
                        if (otherIterator.hasNext() && downstream.canConsume())
                        {
                           BiValue<K, V> next = otherIterator.next();
                           downstream.consume(next.getKey(), next.getValue());
                           return true;
                        }

                        return downstream.produce();
                     }
                  }
               );
            }
         };
      }
   }

   public MapProducable<K, V> merge(final MapProducable<K, V> other)
   {
      return new MapProducable<K, V>()
      {
         @Override
         public Producer producer(final MapTransformer<? super K, ? super V> downstream)
         {
            return MapProducable.this.producer(
               new MapTransformer<K, V>()
               {
                  final Producer otherProducer = other.producer(downstream);

                  @Override
                  public boolean canConsume()
                  {
                     return downstream.canConsume();
                  }

                  @Override
                  public void consume(K k, V v) throws IllegalStateException
                  {
                     downstream.consume(k, v);
                  }

                  @Override
                  public boolean produce()
                  {
                     return downstream.produce() || otherProducer.produce();
                  }
               }
            );
         }
      };
   }

   @Override
   public Producable<K> inputs()
   {
      return new Producable<K>()
      {
         @Override
         public Producer producer(final Transformer<? super K> downstream)
         {
            return MapProducable.this.producer(
               new MapTransformer<K, V>()
               {
                  @Override
                  public boolean canConsume()
                  {
                     return downstream.canConsume();
                  }

                  @Override
                  public void consume(K k, V v) throws IllegalStateException
                  {
                     downstream.consume(k);
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
   public Producable<V> values()
   {
      return new Producable<V>()
      {
         @Override
         public Producer producer(final Transformer<? super V> downstream)
         {
            return MapProducable.this.producer(
               new MapTransformer<K, V>()
               {
                  @Override
                  public boolean canConsume()
                  {
                     return downstream.canConsume();
                  }

                  @Override
                  public void consume(K k, V v) throws IllegalStateException
                  {
                     downstream.consume(v);
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
   public Producable<BiValue<K, V>> asIterable()
   {
      return new Producable<BiValue<K, V>>()
      {
         @Override
         public Producer producer(final Transformer<? super BiValue<K, V>> downstream)
         {
            return MapProducable.this.producer(
               new MapTransformer<K, V>()
               {
                  @Override
                  public boolean canConsume()
                  {
                     return downstream.canConsume();
                  }

                  @Override
                  public void consume(K k, V v) throws IllegalStateException
                  {
                     downstream.consume(new BiVal<>(k, v));
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

   //
   // executing

   private MapTransformer.SingleResultTail<K, V> singleResultTail()
   {
      MapTransformer.SingleResultTail<K, V> singleResultTail
         = new MapTransformer.SingleResultTail<>();

      singleResultTail.setProducer(producer(singleResultTail));

      return singleResultTail;
   }

   private void produceAll(MapTransformer<? super K, ? super V> mapTransformer)
   {
      Producer producer = producer(mapTransformer);
      while (producer.produce()) {}
   }

   @Override
   public boolean isEmpty()
   {
      return !singleResultTail().hasResult();
   }

   @Override
   public BiValue<K, V> getFirst()
   {
      return singleResultTail().getFirstResult();
   }

   @Override
   public BiValue<K, V> getOnly()
   {
      return singleResultTail().getOnlyResult();
   }

   @Override
   public BiValue<K, V> getAny()
   {
      return singleResultTail().getFirstResult();
   }

   @Override
   public <A extends Map<? super K, ? super V>> A into(final A destination)
   {
      produceAll(
         new MapTransformer.Tail<K, V>()
         {
            @Override
            public void consume(K k, V v) throws IllegalStateException
            {
               destination.put(k, v);
            }
         }
      );

      return destination;
   }

   @Override
   public <A extends Map<? super K, C>, C extends Collection<? super V>> A intoMulti(final A destination, final Factory<C> factory)
   {
      produceAll(
         new MapTransformer.Tail<K, V>()
         {
            @Override
            public void consume(K k, V v) throws IllegalStateException
            {
               C group = destination.get(k);
               if (group == null)
                  destination.put(k, group = factory.make());
               group.add(v);
            }
         }
      );

      return destination;
   }

   @Override
   public void forEach(BiBlock<? super K, ? super V> biBlock)
   {
      produceAll(new MapTransformer.BiBlockTail<>(biBlock));
   }

   @Override
   public boolean anyMatch(BiPredicate<? super K, ? super V> biPredicate)
   {
      MapTransformer.FirstMatchTail<K, V> firstMatchTail = new MapTransformer.FirstMatchTail<>(biPredicate);
      return firstMatchTail.isMatch(producer(firstMatchTail));
   }

   @Override
   public boolean noneMatch(BiPredicate<? super K, ? super V> biPredicate)
   {
      return !anyMatch(biPredicate);
   }

   @Override
   public boolean allMatch(BiPredicate<? super K, ? super V> biPredicate)
   {
      return !anyMatch(biPredicate.negate());
   }

   // some additional chain building methods (not in MapStream)

   public <W> MapProducable<K, W> flatMap(final BiMapper<K, V, Iterable<W>> biMapper)
   {
      return new MapProducable<K, W>()
      {
         @Override
         public Producer producer(final MapTransformer<? super K, ? super W> downstream)
         {
            return MapProducable.this.producer(
               new MapTransformer<K, V>()
               {
                  K key;
                  Iterator<W> iterator;

                  @Override
                  public boolean canConsume()
                  {
                     return iterator == null;
                  }

                  @Override
                  public void consume(K k, V v) throws IllegalStateException
                  {
                     if (!canConsume())
                        throw new IllegalStateException("Can't consume while producing");

                     Iterator<W> iterator = biMapper.map(k, v).iterator();
                     this.iterator = iterator.hasNext() ? iterator : null;
                     this.key = this.iterator == null ? null : k;
                  }

                  @Override
                  public boolean produce()
                  {
                     if (iterator != null && downstream.canConsume())
                     {
                        try
                        {
                           downstream.consume(key, iterator.next());
                           return true;
                        }
                        finally
                        {
                           if (!iterator.hasNext())
                           {
                              iterator = null; // early dispose
                              key = null;
                           }
                        }
                     }

                     return downstream.produce();
                  }
               }
            );
         }
      };
   }

   // some additional executing methods (not in MapStream)

   public K getFirstKey()
   {
      return singleResultTail().getFirstKey();
   }

   public K getOnlyKey()
   {
      return singleResultTail().getOnlyKey();
   }

   public K getAnyKey()
   {
      return singleResultTail().getFirstKey();
   }

   public V getFirstValue()
   {
      return singleResultTail().getFirstValue();
   }

   public V getOnlyValue()
   {
      return singleResultTail().getOnlyValue();
   }

   public V getAnyValue()
   {
      return singleResultTail().getFirstValue();
   }
}
