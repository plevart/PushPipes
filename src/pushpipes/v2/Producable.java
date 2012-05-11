package pushpipes.v2;

import java.util.*;
import java.util.functions.*;

/**
 * {@link Producable} is a factory for {@link Producer}s (like {@link Iterable} is a factory for {@link Iterator}s).<p/>
 * Each {@link Producable} is also an {@link Iterable}, but that's only a facade.
 * Internally, Producables can be chained into transformation chains, so that the child producable delegates to it's parent
 * - this facilitates incremental building of transformation chains by fluent API and enables composition:
 * <pre>
 *    producable1 <- producable2 <- ... <- producableN
 * </pre>
 * At evaluation time (when {@code producableN}.{@link #producer(Consumer)} method is called), the chained producables
 * construct a reverse chain of:
 * <pre>
 *    producer1 -> transformer2 -> ... -> transformerN -> consumer
 * </pre>
 * ... and the head {@code producer1} is returned to the caller.
 *
 * @param <T> the type of elements returned/produced by the Producable
 * @author peter.levart@gmail.com
 * @see Producer
 * @see Transformer
 * @see Consumer
 */
public abstract class Producable<T> implements Iterable<T>
{
   public static final int DEFAULT_BUFFER_CAPACITY = 256;

   /**
    * Constructs and returns a chain of:
    * <pre>
    * head {@link Producer} -> ... intermediate (Map)Transformers ... -> downstream {@link Transformer}
    * </pre>
    * ...and returns the head producer.
    *
    * @param downstream a downstream {@link Transformer} that the chain will be connected to
    * @return a head {@link Producer}
    */
   public abstract Producer producer(Transformer<? super T> downstream);

   /**
    * Constructs and returns a chain of:
    * <pre>
    * head {@link Producer} -> ... intermediate (Map)Transformers ... -> {@link Consumer}
    * </pre>
    * ...and returns the head producer.
    *
    * @param consumer a {@link Consumer} that the chain will be connected to
    * @return a head {@link Producer}
    */
   public Producer producer(final Consumer<? super T> consumer)
   {
      if (consumer instanceof Transformer<?>)
         return producer((Transformer<? super T>) consumer);
      else
         return producer(new Transformer.ConsumerTail<>(consumer));
   }

   //
   // factories for head Producables

   public static <T> Producable<T> from(final Iterable<T> iterable)
   {
      return new Producable<T>()
      {
         @Override
         public Producer producer(final Transformer<? super T> downstream)
         {
            return new Producer()
            {
               final Iterator<T> iterator = iterable.iterator();

               @Override
               public boolean produce()
               {
                  if (iterator.hasNext() && downstream.canConsume())
                  {
                     downstream.consume(iterator.next());
                     return true;
                  }

                  return downstream.produce();
               }
            };
         }
      };
   }

   @SafeVarargs
   public static <T> Producable<T> from(final T... array)
   {
      return new Producable<T>()
      {
         @Override
         public Producer producer(final Transformer<? super T> downstream)
         {
            return new Producer()
            {
               int i;

               @Override
               public boolean produce()
               {
                  if (i < array.length && downstream.canConsume())
                  {
                     downstream.consume(array[i++]);
                     return true;
                  }

                  return downstream.produce();
               }
            };
         }
      };
   }

   public static <T> Producable<T> from(final T t)
   {
      return new Producable<T>()
      {
         @Override
         public Producer producer(final Transformer<? super T> downstream)
         {
            return new Producer()
            {
               boolean produced;

               @Override
               public boolean produce()
               {
                  if (!produced && downstream.canConsume())
                  {
                     downstream.consume(t);
                     produced = true;
                     return true;
                  }

                  return downstream.produce();
               }
            };
         }
      };
   }

   //
   // chain building

   @Override
   public Producable<T> filter(final Predicate<? super T> predicate)
   {
      return new Producable<T>()
      {
         @Override
         public Producer producer(final Transformer<? super T> downstream)
         {
            return Producable.this.producer(
               new Transformer<T>()
               {
                  @Override
                  public boolean canConsume()
                  {
                     return downstream.canConsume();
                  }

                  @Override
                  public void consume(T t) throws IllegalStateException
                  {
                     if (predicate.test(t))
                        downstream.consume(t);
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
   public <U> Producable<U> map(final Mapper<? super T, ? extends U> mapper)
   {
      return new Producable<U>()
      {
         @Override
         public Producer producer(final Transformer<? super U> downstream)
         {
            return Producable.this.producer(
               new Transformer<T>()
               {
                  @Override
                  public boolean canConsume()
                  {
                     return downstream.canConsume();
                  }

                  @Override
                  public void consume(T t)
                  {
                     downstream.consume(mapper.map(t));
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
   public <U> Producable<U> flatMap(final Mapper<? super T, ? extends Iterable<U>> mapper)
   {
      return new Producable<U>()
      {
         @Override
         public Producer producer(final Transformer<? super U> downstream)
         {
            return Producable.this.producer(
               new Transformer<T>()
               {
                  Iterator<U> iterator;

                  @Override
                  public boolean canConsume()
                  {
                     return iterator == null;
                  }

                  @Override
                  public void consume(T t) throws IllegalStateException
                  {
                     if (!canConsume())
                        throw new IllegalStateException("Can't consume while producing");

                     Iterator<U> iterator = mapper.map(t).iterator();
                     this.iterator = iterator.hasNext() ? iterator : null;
                  }

                  @Override
                  public boolean produce()
                  {
                     if (iterator != null && downstream.canConsume())
                     {
                        try
                        {
                           downstream.consume(iterator.next());
                           return true;
                        }
                        finally
                        {
                           if (!iterator.hasNext()) iterator = null; // early dispose
                        }
                     }

                     return downstream.produce();
                  }
               }
            );
         }
      };
   }

   @Override
   public Producable<T> sorted(final Comparator<? super T> comparator)
   {
      return new Producable<T>()
      {
         @Override
         public Producer producer(final Transformer<? super T> downstream)
         {
            return Producable.this.producer(
               new Transformer<T>()
               {
                  @SuppressWarnings("unchecked")
                  T[] array = (T[]) new Object[DEFAULT_BUFFER_CAPACITY];
                  int size;
                  boolean sorted;

                  @Override
                  public boolean canConsume()
                  {
                     return !sorted;
                  }

                  @Override
                  public void consume(T t) throws IllegalStateException
                  {
                     if (!canConsume())
                        throw new IllegalStateException("Can not consume after already sorting and producing output");

                     if (size >= array.length)
                        array = Arrays.copyOf(array, array.length << 1);

                     array[size++] = t;
                  }

                  int i;

                  @Override
                  public boolean produce()
                  {
                     if (!sorted)
                     {
                        Arrays.sort(array, 0, size, comparator);
                        sorted = true;
                     }

                     if (i < size && downstream.canConsume())
                     {
                        downstream.consume(array[i++]);
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
   public Producable<T> uniqueElements()
   {
      return new Producable<T>()
      {
         @Override
         public Producer producer(final Transformer<? super T> downstream)
         {
            return Producable.this.producer(
               new Transformer<T>()
               {
                  Set<T> set = new HashSet<>();
                  Iterator<T> iterator;

                  @Override
                  public boolean canConsume()
                  {
                     return iterator == null;
                  }

                  @Override
                  public void consume(T t) throws IllegalStateException
                  {
                     if (!canConsume())
                        throw new IllegalStateException("Can't consume while producing");

                     set.add(t);
                  }

                  @Override
                  public boolean produce()
                  {
                     if (iterator == null)
                        iterator = set.iterator();

                     if (iterator.hasNext() && downstream.canConsume())
                     {
                        downstream.consume(iterator.next());
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
   public Producable<T> cumulate(final BinaryOperator<T> op)
   {
      return new Producable<T>()
      {
         @Override
         public Producer producer(final Transformer<? super T> downstream)
         {
            return Producable.this.producer(
               new Transformer<T>()
               {
                  T last;
                  boolean first = true;

                  @Override
                  public boolean canConsume()
                  {
                     return downstream.canConsume();
                  }

                  @Override
                  public void consume(T t) throws IllegalStateException
                  {
                     if (first)
                     {
                        last = t;
                        first = false;
                     }
                     else
                     {
                        last = op.eval(last, t);
                     }

                     downstream.consume(last);
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
   public <U> MapProducable<T, U> mapped(final Mapper<? super T, ? extends U> mapper)
   {
      return new MapProducable<T, U>()
      {
         @Override
         public Producer producer(final MapTransformer<? super T, ? super U> downstream)
         {
            return Producable.this.producer(
               new Transformer<T>()
               {
                  @Override
                  public boolean canConsume()
                  {
                     return downstream.canConsume();
                  }

                  @Override
                  public void consume(T t) throws IllegalStateException
                  {
                     downstream.consume(t, mapper.map(t));
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
   public <U> MapProducable<U, Iterable<T>> groupBy(final Mapper<? super T, ? extends U> mapper)
   {
      return new MapProducable<U, Iterable<T>>()
      {
         @Override
         public Producer producer(final MapTransformer<? super U, ? super Iterable<T>> downstream)
         {
            return Producable.this.producer(
               new Transformer<T>()
               {
                  Map<U, Collection<T>> multiMap = new HashMap<>();
                  Iterator<Map.Entry<U, Collection<T>>> iterator;

                  @Override
                  public boolean canConsume()
                  {
                     return iterator == null;
                  }

                  @Override
                  public void consume(T t) throws IllegalStateException
                  {
                     if (!canConsume())
                        throw new IllegalStateException("Can't consume while producing");

                     U key = mapper.map(t);

                     Collection<T> group = multiMap.get(key);
                     if (group == null)
                        multiMap.put(key, group = new ArrayList<>());

                     group.add(t);
                  }

                  @Override
                  public boolean produce()
                  {
                     if (iterator == null)
                        iterator = multiMap.entrySet().iterator();

                     if (iterator.hasNext() && downstream.canConsume())
                     {
                        Map.Entry<U, Collection<T>> entry = iterator.next();
                        downstream.consume(entry.getKey(), entry.getValue());
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
   public <U> MapProducable<U, Iterable<T>> groupByMulti(final Mapper<? super T, ? extends Iterable<U>> mapper)
   {
      return new MapProducable<U, Iterable<T>>()
      {
         @Override
         public Producer producer(final MapTransformer<? super U, ? super Iterable<T>> downstream)
         {
            return Producable.this.producer(
               new Transformer<T>()
               {
                  Map<U, Collection<T>> multiMap = new HashMap<>();
                  Iterator<Map.Entry<U, Collection<T>>> iterator;

                  @Override
                  public boolean canConsume()
                  {
                     return iterator == null;
                  }

                  @Override
                  public void consume(T t) throws IllegalStateException
                  {
                     if (!canConsume())
                        throw new IllegalStateException("Can't consume while producing");

                     for (U key : mapper.map(t))
                     {
                        Collection<T> group = multiMap.get(key);
                        if (group == null)
                           multiMap.put(key, group = new ArrayList<>());

                        group.add(t);
                     }
                  }

                  @Override
                  public boolean produce()
                  {
                     if (iterator == null)
                        iterator = multiMap.entrySet().iterator();

                     if (iterator.hasNext() && downstream.canConsume())
                     {
                        Map.Entry<U, Collection<T>> entry = iterator.next();
                        downstream.consume(entry.getKey(), entry.getValue());
                        return true;
                     }

                     return downstream.produce();
                  }
               }
            );
         }
      };
   }

   //
   // execution

   private Transformer.SingleResultTail<T> singleResultTail()
   {
      Transformer.SingleResultTail<T> singleResultTail
         = new Transformer.SingleResultTail<>();

      singleResultTail.setProducer(producer(singleResultTail));

      return singleResultTail;
   }

   private void produceAll(Transformer<? super T> transformer)
   {
      Producer producer = producer(transformer);
      while (producer.produce()) {}
   }

   @Override
   public boolean isEmpty()
   {
      return !singleResultTail().hasResult();
   }

   @Override
   public T getFirst()
   {
      return singleResultTail().getFirstResult();
   }

   @Override
   public T getOnly()
   {
      return singleResultTail().getOnlyResult();
   }

   @Override
   public T getAny()
   {
      return singleResultTail().getFirstResult();
   }

   @Override
   public long count()
   {
      Transformer.ResultCounterTail counter = new Transformer.ResultCounterTail();
      return counter.getResultCount(producer(counter));
   }

   @Override
   public boolean anyMatch(Predicate<? super T> filter)
   {
      Transformer.FirstMatchTail<T> firstMatchTail = new Transformer.FirstMatchTail<>(filter);
      return firstMatchTail.isMatch(producer(firstMatchTail));
   }

   @Override
   public boolean noneMatch(Predicate<? super T> filter)
   {
      return !anyMatch(filter);
   }

   @Override
   public boolean allMatch(Predicate<? super T> filter)
   {
      return !anyMatch(filter.negate());
   }

   @Override
   public void forEach(Block<? super T> block)
   {
      produceAll(new Transformer.BlockTail<>(block));
   }

   @Override
   public Iterator<T> iterator()
   {
      Transformer.IteratorTail<T> iterator
         = new Transformer.IteratorTail<>();

      iterator.setProducer(producer(iterator));

      return iterator;
   }

   @Override
   public T reduce(T base, BinaryOperator<T> reducer)
   {
      Transformer.ReducerTail<T> reducerTail = new Transformer.ReducerTail<>(base, reducer);
      return reducerTail.getResult(producer(reducerTail));
   }

   @Override
   public <U> U mapReduce(Mapper<? super T, ? extends U> mapper, U base, BinaryOperator<U> reducer)
   {
      return map(mapper).reduce(base, reducer);
   }

   @Override
   public int mapReduce(IntMapper<? super T> intMapper, int base, IntBinaryOperator reducer)
   {
      Transformer.IntReducerTail<T> reducerTail = new Transformer.IntReducerTail<>(intMapper, base, reducer);
      return reducerTail.getResult(producer(reducerTail));
   }

   @Override
   public long mapReduce(LongMapper<? super T> longMapper, long base, LongBinaryOperator reducer)
   {
      Transformer.LongReducerTail<T> reducerTail = new Transformer.LongReducerTail<>(longMapper, base, reducer);
      return reducerTail.getResult(producer(reducerTail));
   }

   @Override
   public double mapReduce(DoubleMapper<? super T> doubleMapper, double base, DoubleBinaryOperator reducer)
   {
      Transformer.DoubleReducerTail<T> reducerTail = new Transformer.DoubleReducerTail<>(doubleMapper, base, reducer);
      return reducerTail.getResult(producer(reducerTail));
   }

   @Override
   public <A extends Fillable<? super T>> A into(A target)
   {
      // reuse our Iterable facade...
      return Iterables.into(this, target);
   }
}
