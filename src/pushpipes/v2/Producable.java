package pushpipes.v2;

import java.util.*;
import java.util.functions.*;

/**
 * @author peter.levart@gmail.com
 */
public abstract class Producable<T> implements Iterable<T>
{
   public static final int DEFAULT_BUFFER_CAPACITY = 256;

   /**
    * Constructs and returns a chain of:<p/>
    * head {@link Producer} -> ... intermediate ... -> downstream {@link ConsumerProducer}<p/>
    * ...and returns the head producer.
    *
    * @param downstream a downstream {@link ConsumerProducer} that the chain will be connected to
    * @return a head {@link Producer}
    */
   public abstract Producer producer(ConsumerProducer<? super T> downstream);

   //
   // factories for head Producables

   public static <T> Producable<T> from(Iterable<T> iterable)
   {
      return new IterableProducable<>(iterable);
   }

   @SafeVarargs
   public static <T> Producable<T> from(T... array)
   {
      return new ArrayProducable<>(array);
   }

   public static <T> Producable<T> from(T singleValue)
   {
      return new SingletonProducable<>(singleValue);
   }

   //
   // chain building

   public Producable<T> filter(final Predicate<? super T> predicate)
   {
      return new Producable<T>()
      {
         @Override
         public Producer producer(final ConsumerProducer<? super T> downstream)
         {
            return Producable.this.producer(
               new ConsumerProducer<T>()
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

   public <U> Producable<U> map(final Mapper<? super T, ? extends U> mapper)
   {
      return new Producable<U>()
      {
         @Override
         public Producer producer(final ConsumerProducer<? super U> downstream)
         {
            return Producable.this.producer(
               new ConsumerProducer<T>()
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

   public <U> Producable<U> flatMap(final Mapper<? super T, ? extends Iterable<U>> mapper)
   {
      return new Producable<U>()
      {
         @Override
         public Producer producer(final ConsumerProducer<? super U> downstream)
         {
            return Producable.this.producer(
               new ConsumerProducer<T>()
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
                     if (iterator != null)
                     {
                        try
                        {
                           if (iterator.hasNext() && downstream.canConsume())
                           {
                              downstream.consume(iterator.next());
                              return true;
                           }
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
         public Producer producer(final ConsumerProducer<? super T> downstream)
         {
            return Producable.this.producer(
               new ConsumerProducer<T>()
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
         public Producer producer(final ConsumerProducer<? super T> downstream)
         {
            return Producable.this.producer(
               new ConsumerProducer<T>()
               {
                  Set<T> set = new HashSet<T>();
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
   public <U> MapProducable<T, U> mapped(Mapper<? super T, ? extends U> mapper)
   {
      return null; //super.mapped(mapper);
   }

   //
   // execution

   public long count()
   {
      ConsumerProducer.Counter counter = new ConsumerProducer.Counter();
      Producer producer = producer(counter);
      while (producer.produce()) {}
      return counter.getCount();
   }

   @Override
   public Iterator<T> iterator()
   {
      ConsumerProducer.IteratorTail<T> iterator
         = new ConsumerProducer.IteratorTail<>();

      iterator.setProducer(producer(iterator));

      return iterator;
   }

   //
   // head Producable implementations

   private static class ArrayProducable<T> extends Producable<T>
   {
      private final T[] array;

      @SafeVarargs
      private ArrayProducable(T... array)
      {
         this.array = array;
      }

      @Override
      public Producer producer(final ConsumerProducer<? super T> downstream)
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
   }

   private static class IterableProducable<T> extends Producable<T>
   {
      private final Iterable<T> iterable;

      private IterableProducable(Iterable<T> iterable)
      {
         this.iterable = iterable;
      }

      @Override
      public Producer producer(final ConsumerProducer<? super T> downstream)
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
   }

   private static class SingletonProducable<T> extends Producable<T>
   {
      private final T t;

      private SingletonProducable(T t)
      {
         this.t = t;
      }

      @Override
      public Producer producer(final ConsumerProducer<? super T> downstream)
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
   }
}
