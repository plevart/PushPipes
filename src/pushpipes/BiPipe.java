package pushpipes;

import java.util.*;
import java.util.functions.*;

/**
 * @author peter.levart@gmail.com
 */
public abstract class BiPipe<T, U> extends AbstractPipe<BiBlock<? super T, ? super U>>
{
   //
   // constructing

   public static <T, U> BiPipe<T, U> from(Map<T, U> map)
   {
      return new MapPipe<>(map);
   }

   //
   // pipe chain building...

   public Pipe<T> keys()
   {
      return new Step<T, U, T>(this)
      {
         @Override
         public void apply(T t, U u)
         {
            downstream.apply(t);
         }
      };
   }

   public Pipe<U> values()
   {
      return new Step<T, U, U>(this)
      {
         @Override
         public void apply(T t, U u)
         {
            downstream.apply(u);
         }
      };
   }

   public BiPipe<U, T> swap()
   {
      return new BiStep<T, U, U, T>(this)
      {
         @Override
         public void apply(T t, U u)
         {
            downstream.apply(u, t);
         }
      };
   }

   public BiPipe<T, U> filterKeys(final Predicate<? super T> keyPredicate)
   {
      return new BiStep<T, U, T, U>(this)
      {
         @Override
         public void apply(T t, U u)
         {
            if (keyPredicate.test(t))
               downstream.apply(t, u);
         }
      };
   }

   public BiPipe<T, U> filterValues(final Predicate<? super U> valuePredicate)
   {
      return new BiStep<T, U, T, U>(this)
      {
         @Override
         public void apply(T t, U u)
         {
            if (valuePredicate.test(u))
               downstream.apply(t, u);
         }
      };
   }

   public BiPipe<T, U> filter(final BiPredicate<? super T, ? super U> predicate)
   {
      return new BiStep<T, U, T, U>(this)
      {
         @Override
         public void apply(T t, U u)
         {
            if (predicate.eval(t, u))
               downstream.apply(t, u);
         }
      };
   }

   public <V> BiPipe<T, V> map(final BiMapper<? super T, ? super U, ? extends V> mapper)
   {
      return new BiStep<T, U, T, V>(this)
      {
         @Override
         public void apply(T t, U u)
         {
            downstream.apply(t, mapper.map(t, u));
         }
      };
   }

   public <V> BiPipe<T, V> mapValues(final Mapper<? super U, ? extends V> valueMapper)
   {
      return new BiStep<T, U, T, V>(this)
      {
         @Override
         public void apply(T t, U u)
         {
            downstream.apply(t, valueMapper.map(u));
         }
      };
   }

   public <V> BiPipe<T, V> flatMap(final BiMapper<? super T, ? super U, ? extends Iterable<V>> mapper)
   {
      return new BiStep<T, U, T, V>(this)
      {
         @Override
         public void apply(T t, U u)
         {
            for (V v : mapper.map(t, u))
               downstream.apply(t, v);
         }
      };
   }

   public BiPipe<T, ? extends Iterable<U>> asMulti()
   {
      return new BiStep<T, U, T, Iterable<U>>(this)
      {
         @Override
         public void apply(T t, U u)
         {
            downstream.apply(t, Collections.singletonList(u));
         }
      };
   }

   public BiPipe<T, ? extends Iterable<U>> groupByKey()
   {
      return groupByKeyValuesInto(
         new Factory<ArrayList<U>>()
         {
            @Override
            public ArrayList<U> make()
            {
               return new ArrayList<>();
            }
         }
      );
   }

   public <C extends Collection<U>> BiPipe<T, C> groupByKeyValuesInto(final Factory<C> valuesCollectionFactory)
   {
      return new BiStep<T, U, T, C>(this)
      {
         private Map<T, C> multiMap;

         @Override
         public void apply(T t, U u)
         {
            C values = multiMap.get(t);
            if (values == null)
            {
               values = valuesCollectionFactory.make();
               multiMap.put(t, values);
            }
            values.add(u);
         }

         @Override
         protected void process()
         {
            multiMap = new HashMap<>();

            try
            {
               super.process();

               for (Map.Entry<T, C> entry : multiMap.entrySet())
               {
                  downstream.apply(entry.getKey(), entry.getValue());
               }
            }
            finally
            {
               multiMap.clear();
               multiMap = null;
            }
         }
      };
   }

   // TODO: optimize this!
   public <C extends Comparable<C>> BiPipe<T, U> sortBy(final BiMapper<? super T, ? super U, C> sortKeyExtractor)
   {
      class TUC implements Comparable<TUC>
      {
         final T t;
         final U u;
         final C c;

         TUC(T t, U u, C c)
         {
            this.t = t;
            this.u = u;
            this.c = c;
         }

         @Override
         public int compareTo(TUC other) { return this.c.compareTo(other.c); }
      }

      return new BiStep<T, U, T, U>(this)
      {
         private Object[] array;
         private int size;

         @Override
         public void apply(T t, U u)
         {
            if (size >= array.length)
               array = Arrays.copyOf(array, array.length * 2);

            array[size++] = new TUC(t, u, sortKeyExtractor.map(t, u));
         }

         @Override
         protected void process()
         {
            array = new Object[DEFAULT_BUFFER_CAPACITY];
            size = 0;

            try
            {
               super.process();

               Arrays.sort(array, 0, size);

               for (int i = 0; i < size; i++)
               {
                  TUC tuc = (TUC) array[i];
                  downstream.apply(tuc.t, tuc.u);
               }
            }
            finally
            {
               array = null;
            }
         }
      };
   }

   //
   // pipe chain executing

   public long count()
   {
      BiCounter counter = new BiCounter();
      AbstractPipe<BiBlock<? super T, ? super U>> pipe = connect(counter);
      try
      {
         pipe.process();
         return counter.getCount();
      }
      finally
      {
         pipe.disconnect(counter);
      }
   }

   public void forEach(BiBlock<? super T, ? super U> block)
   {
      AbstractPipe<BiBlock<? super T, ? super U>> pipe = connect(block);
      try
      {
         pipe.process();
      }
      finally
      {
         pipe.disconnect(block);
      }
   }

   public <A extends Map<? super T, ? super U>> A into(final A destination)
   {
      forEach(new BiBlock<T, U>()
      {
         @Override
         public void apply(T t, U u)
         {
            destination.put(t, u);
         }
      });

      return destination;
   }

   public BiValue<T, U> getFirst()
   {
      SingleResultBiBlock<T, U> collector = new SingleResultBiBlock<>(true);
      try
      {
         return process(collector);
      }
      catch (LongBreak lb)
      {
         return collector.getResult();
      }
   }

   public BiValue<T, U> getSingle()
   {
      return process(new SingleResultBiBlock<T, U>(false));
   }

   public T getFirstKey()
   {
      return getFirst().getKey();
   }

   public T getSingleKey()
   {
      return getSingle().getKey();
   }

   public U getFirstValue()
   {
      return getFirst().getValue();
   }

   public U getSingleValue()
   {
      return getSingle().getValue();
   }

   private BiValue<T, U> process(ResultBiBlock<T, U> resultBiBlock)
   {
      AbstractPipe<BiBlock<? super T, ? super U>> pipe = connect(resultBiBlock);
      try
      {
         pipe.process();
         return resultBiBlock.getResult();
      }
      finally
      {
         pipe.disconnect(resultBiBlock);
      }
   }

   public boolean anyMatch(BiPredicate<? super T, ? super U> predicate)
   {
      BreakOnFirstMatch<T, U> breakOnFirstMatch = new BreakOnFirstMatch<>(predicate);
      AbstractPipe<BiBlock<? super T, ? super U>> pipe = connect(breakOnFirstMatch);
      try
      {
         pipe.process();
         return false;
      }
      catch (LongBreak lb)
      {
         return true;
      }
      finally
      {
         pipe.disconnect(breakOnFirstMatch);
      }
   }

   public boolean allMatch(BiPredicate<? super T, ? super U> predicate)
   {
      BreakOnFirstMatch<T, U> breakOnFirstNonMatch = new BreakOnFirstMatch<>(predicate.negate());
      AbstractPipe<BiBlock<? super T, ? super U>> pipe = connect(breakOnFirstNonMatch);
      try
      {
         pipe.process();
         return true;
      }
      catch (LongBreak lb)
      {
         return false;
      }
      finally
      {
         pipe.disconnect(breakOnFirstNonMatch);
      }
   }

   public boolean noneMatch(BiPredicate<? super T, ? super U> predicate)
   {
      return !anyMatch(predicate);
   }

   //
   // pipe chain elements...

   static class MapPipe<T, U> extends BiPipe<T, U>
   {
      private final Map<T, U> map;

      MapPipe(Map<T, U> map)
      {
         this.map = map;
      }

      @Override
      protected void process()
      {
         for (Map.Entry<T, U> entry : map.entrySet())
            downstream.apply(entry.getKey(), entry.getValue());
      }
   }

   static abstract class BiStep<TI, UI, TO, UO> extends BiPipe<TO, UO> implements BiBlock<TI, UI>
   {
      private final AbstractPipe<BiBlock<? super TI, ? super UI>> upstream;

      BiStep(BiPipe<TI, UI> upstream)
      {
         this.upstream = upstream.connect(this);
      }

      @Override
      protected void process()
      {
         upstream.process();
      }
   }

   static abstract class Step<TI, UI, O> extends Pipe<O> implements BiBlock<TI, UI>
   {
      private final AbstractPipe<BiBlock<? super TI, ? super UI>> upstream;

      Step(BiPipe<TI, UI> upstream)
      {
         this.upstream = upstream.connect(this);
      }

      @Override
      protected void process()
      {
         upstream.process();
      }
   }

   static class BiCounter implements BiBlock<Object, Object>
   {
      private long count;

      @Override
      public void apply(Object o, Object o1)
      {
         count++;
      }

      long getCount()
      {
         return count;
      }
   }

   abstract static class ResultBiBlock<T, U> implements BiBlock<T, U>, BiValue<T, U>
   {
      protected static final Object NONE = new Object();
      protected T key;
      protected U value;

      @SuppressWarnings("unchecked")
      ResultBiBlock()
      {
         this((T) NONE, (U) NONE);
      }

      ResultBiBlock(T key, U value)
      {
         this.key = key;
         this.value = value;
      }

      public T getKey()
      {
         return key;
      }

      public U getValue()
      {
         return value;
      }

      BiValue<T, U> getResult()
      {
         if (key == NONE || value == NONE)
            throw new NoSuchElementException("No result");

         return this;
      }
   }

   static class SingleResultBiBlock<T, U> extends ResultBiBlock<T, U>
   {
      private final boolean breakAfterFirstResult;

      SingleResultBiBlock(boolean breakAfterFirstResult)
      {
         this.breakAfterFirstResult = breakAfterFirstResult;
      }

      @Override
      public void apply(T t, U u)
      {
         if (key != NONE || value != NONE)
            throw new IllegalStateException("Multiple results");

         key = t;
         value = u;

         if (breakAfterFirstResult)
            throw LongBreak.INSTANCE;
      }
   }

   static class BreakOnFirstMatch<T, U> implements BiBlock<T, U>
   {
      final BiPredicate<? super T, ? super U> predicate;

      BreakOnFirstMatch(BiPredicate<? super T, ? super U> predicate)
      {
         this.predicate = predicate;
      }

      @Override
      public void apply(T t, U u)
      {
         if (predicate.eval(t, u))
            throw LongBreak.INSTANCE;
      }
   }

   private static class LongBreak extends RuntimeException
   {
      static final LongBreak INSTANCE = new LongBreak();

      private LongBreak()
      {
      }

      @Override
      public synchronized Throwable fillInStackTrace()
      {
         return this;
      }
   }
}
