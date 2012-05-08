package pushpipes;

import java.util.*;
import java.util.functions.*;

/**
 * @author peter.levart@gmail.com
 */
public abstract class Pipe<T> extends AbstractPipe<Block<? super T>>
{
   //
   // constructing

   public static <T> Pipe<T> from(Iterable<T> iterable)
   {
      return new IterablePipe<>(iterable);
   }

   public static <T> Pipe<T> from(T value)
   {
      return new SingletonPipe<>(value);
   }

   @SafeVarargs
   public static <T> Pipe<T> from(T... values)
   {
      return new ArrayPipe<>(values);
   }

   //
   // pipe chain building...

   public Pipe<T> filter(final Predicate<? super T> predicate)
   {
      return new Step<T, T>(this)
      {
         public void apply(T arg)
         {
            if (predicate.test(arg))
               downstream.apply(arg);
         }
      };
   }

   public <U> Pipe<U> map(final Mapper<? super T, ? extends U> mapper)
   {
      return new Step<T, U>(this)
      {
         public void apply(T arg)
         {
            downstream.apply(mapper.map(arg));
         }
      };
   }

   /**
    * This makes sense only in the push pipes...
    */
   public <U> Pipe<U> map(final BiBlock<? super T, Block<? super U>> pushMapper)
   {
      return new Step<T, U>(this)
      {
         public void apply(T arg)
         {
            pushMapper.apply(arg, downstream);
         }
      };
   }

   public <U> Pipe<U> flatMap(final Mapper<? super T, ? extends Iterable<U>> mapper)
   {
      return new Step<T, U>(this)
      {
         public void apply(T arg)
         {
            for (U element : mapper.map(arg))
               downstream.apply(element);
         }
      };
   }

   public IntPipe mapInt(final IntMapper<? super T> mapper)
   {
      return new IntStep<T>(this)
      {
         public void apply(T arg)
         {
            downstream.apply(mapper.map(arg));
         }
      };
   }

   public LongPipe mapLong(final LongMapper<? super T> mapper)
   {
      return new LongStep<T>(this)
      {
         public void apply(T arg)
         {
            downstream.apply(mapper.map(arg));
         }
      };
   }

   public DoublePipe mapDouble(final DoubleMapper<? super T> mapper)
   {
      return new DoubleStep<T>(this)
      {
         public void apply(T arg)
         {
            downstream.apply(mapper.map(arg));
         }
      };
   }

   public Pipe<T> cumulate(final BinaryOperator<T> op)
   {
      return new Step<T, T>(this)
      {
         private boolean first;
         private T next;

         @Override
         public void apply(T t)
         {
            if (first)
            {
               next = t;
               first = false;
            }
            else
            {
               next = op.eval(next, t);
            }

            downstream.apply(next);
         }

         @Override
         protected void process()
         {
            first = true;
            try
            {
               super.process();
            }
            finally
            {
               next = null;
            }
         }
      };
   }

   public Pipe<T> sorted()
   {
      return sorted(null);
   }

   public Pipe<T> sorted(final Comparator<? super T> comparator)
   {
      return new Step<T, T>(this)
      {
         PriorityQueue<T> pq;

         @Override
         public void apply(T t)
         {
            pq.add(t);
         }

         @Override
         protected void process()
         {
            pq = comparator == null
                 ? new PriorityQueue<T>(DEFAULT_BUFFER_CAPACITY)
                 : new PriorityQueue<T>(DEFAULT_BUFFER_CAPACITY, comparator);

            try
            {
               super.process();

               while (!pq.isEmpty())
                  downstream.apply(pq.remove());
            }
            finally
            {
               pq.clear();
               pq = null;
            }
         }
      };
   }

   public <C extends Comparable<C>> Pipe<T> sortBy(final Mapper<? super T, C> sortKeyExtractor)
   {
      return sorted(
         new Comparator<T>()
         {
            @Override
            public int compare(T t1, T t2)
            {
               return
                  sortKeyExtractor.map(t1)
                     .compareTo(sortKeyExtractor.map(t2));
            }
         }
      );
   }

   public Pipe<T> uniqueElements()
   {
      return new Step<T, T>(this)
      {
         Set<T> seen;

         @Override
         public void apply(T t)
         {
            if (seen.add(t))
               downstream.apply(t);
         }

         @Override
         protected void process()
         {
            seen = new HashSet<>(DEFAULT_BUFFER_CAPACITY * 4 / 3 + 1, 0.75f);
            try
            {
               super.process();
            }
            finally
            {
               seen.clear();
               seen = null;
            }
         }
      };
   }

   public <U> BiPipe<T, U> mapped(final Mapper<? super T, ? extends U> mapper)
   {
      return new BiStep<T, T, U>(this)
      {
         @Override
         public void apply(T t)
         {
            downstream.apply(t, mapper.map(t));
         }
      };
   }

   public <U> BiPipe<U, Iterable<T>> groupBy(final Mapper<? super T, ? extends U> mapper)
   {
      return new BiStep<T, U, Iterable<T>>(this)
      {
         private Map<U, Collection<T>> multiMap;

         @Override
         public void apply(T t)
         {
            U key = mapper.map(t);
            Collection<T> values = multiMap.get(key);
            if (values == null)
            {
               values = new ArrayList<>();
               multiMap.put(key, values);
            }
            values.add(t);
         }

         @Override
         protected void process()
         {
            multiMap = new HashMap<>();

            try
            {
               super.process();

               for (Map.Entry<U, Collection<T>> entry : multiMap.entrySet())
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

   public <U> BiPipe<U, ? extends Iterable<T>> groupByMulti(final Mapper<? super T, ? extends Iterable<U>> mapper)
   {
      return new BiStep<T, U, Iterable<T>>(this)
      {
         private Map<U, Collection<T>> multiMap;

         @Override
         public void apply(T t)
         {
            for (U key : mapper.map(t))
            {
               Collection<T> values = multiMap.get(key);
               if (values == null)
               {
                  values = new ArrayList<>();
                  multiMap.put(key, values);
               }
               values.add(t);
            }
         }

         @Override
         protected void process()
         {
            multiMap = new HashMap<>();

            try
            {
               super.process();

               for (Map.Entry<U, Collection<T>> entry : multiMap.entrySet())
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

   //
   // pipe chain executing

   public long count()
   {
      Counter counter = new Counter();
      AbstractPipe<Block<? super T>> pipe = connect(counter);
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

   public void forEach(Block<? super T> block)
   {
      AbstractPipe<Block<? super T>> pipe = connect(block);
      try
      {
         pipe.process();
      }
      finally
      {
         pipe.disconnect(block);
      }
   }

   public <A extends Fillable<? super T>> A into(final A target)
   {
      BufferBlock<T> bufferBlock = new BufferBlock<T>()
      {
         @Override
         protected void flush(Iterable<T> buffer)
         {
            target.addAll(buffer);
         }
      };

      forEach(bufferBlock);
      bufferBlock.flush();

      return target;
   }

   public T getFirst()
   {
      SingleResultBlock<T> collector = new SingleResultBlock<>(true);
      try
      {
         return process(collector);
      }
      catch (LongBreak lb)
      {
         return collector.getResult();
      }
   }

   public T getSingle()
   {
      return process(new SingleResultBlock<T>(false));
   }

   public T reduce(BinaryOperator<T> reducer)
   {
      return process(new ReducerBlock<>(reducer));
   }

   public T reduce(T base, BinaryOperator<T> reducer)
   {
      return process(new ReducerBlock<>(base, reducer));
   }

   private T process(ResultBlock<T> resultBlock)
   {
      AbstractPipe<Block<? super T>> pipe = connect(resultBlock);
      try
      {
         pipe.process();
         return resultBlock.getResult();
      }
      finally
      {
         pipe.disconnect(resultBlock);
      }
   }

   //
   // pipe chain elements...

   static class IterablePipe<T> extends Pipe<T>
   {
      private final Iterable<T> iterable;

      IterablePipe(Iterable<T> iterable)
      {
         this.iterable = iterable;
      }

      @Override
      protected void process()
      {
         for (T element : iterable)
            downstream.apply(element);
      }
   }

   static class SingletonPipe<T> extends Pipe<T>
   {
      private final T value;

      SingletonPipe(T value)
      {
         this.value = value;
      }

      @Override
      protected void process()
      {
         downstream.apply(value);
      }
   }

   static class ArrayPipe<T> extends Pipe<T>
   {
      private final T[] values;

      ArrayPipe(T[] values)
      {
         this.values = values;
      }

      @Override
      protected void process()
      {
         for (T value : values)
            downstream.apply(value);
      }
   }

   static abstract class Step<I, O> extends Pipe<O> implements Block<I>
   {
      private final AbstractPipe<Block<? super I>> upstream;

      Step(Pipe<I> upstream)
      {
         this.upstream = upstream.connect(this);
      }

      @Override
      protected void process()
      {
         upstream.process();
      }
   }

   static abstract class BiStep<I, OT, OU> extends BiPipe<OT, OU> implements Block<I>
   {
      private final AbstractPipe<Block<? super I>> upstream;

      BiStep(Pipe<I> upstream)
      {
         this.upstream = upstream.connect(this);
      }

      @Override
      protected void process()
      {
         upstream.process();
      }
   }

   static abstract class IntStep<I> extends IntPipe implements Block<I>
   {
      private final AbstractPipe<Block<? super I>> upstream;

      IntStep(Pipe<I> upstream)
      {
         this.upstream = upstream.connect(this);
      }

      @Override
      protected void process()
      {
         upstream.process();
      }
   }

   static abstract class LongStep<I> extends LongPipe implements Block<I>
   {
      private final AbstractPipe<Block<? super I>> upstream;

      LongStep(Pipe<I> upstream)
      {
         this.upstream = upstream.connect(this);
      }

      @Override
      protected void process()
      {
         upstream.process();
      }
   }

   static abstract class DoubleStep<I> extends DoublePipe implements Block<I>
   {
      private final AbstractPipe<Block<? super I>> upstream;

      DoubleStep(Pipe<I> upstream)
      {
         this.upstream = upstream.connect(this);
      }

      @Override
      protected void process()
      {
         upstream.process();
      }
   }

   static class Counter implements Block<Object>
   {
      private long count;

      public void apply(Object o)
      {
         count++;
      }

      long getCount()
      {
         return count;
      }
   }


   abstract static class ResultBlock<T> implements Block<T>
   {
      protected static final Object NONE = new Object();
      protected T result;

      @SuppressWarnings("unchecked")
      ResultBlock()
      {
         this((T) NONE);
      }

      ResultBlock(T result)
      {
         this.result = result;
      }

      T getResult()
      {
         if (result == NONE)
            throw new NoSuchElementException("No result");

         return result;
      }
   }

   static class SingleResultBlock<T> extends ResultBlock<T>
   {
      private final boolean breakAfterFirstResult;

      SingleResultBlock(boolean breakAfterFirstResult)
      {
         this.breakAfterFirstResult = breakAfterFirstResult;
      }

      public void apply(T t)
      {
         if (result != NONE)
            throw new IllegalStateException("Multiple results");

         result = t;

         if (breakAfterFirstResult)
            throw LongBreak.INSTANCE;
      }
   }

   static class ReducerBlock<T> extends ResultBlock<T>
   {
      private final BinaryOperator<T> reducer;

      @SuppressWarnings("unchecked")
      ReducerBlock(BinaryOperator<T> reducer)
      {
         super();
         this.reducer = reducer;
      }

      ReducerBlock(T base, BinaryOperator<T> reducer)
      {
         super(base);
         this.reducer = reducer;
      }

      public void apply(T t)
      {
         result = result == NONE ? t : reducer.eval(result, t);
      }
   }

   static abstract class BufferBlock<T> implements Block<T>
   {
      private final int capacity;
      private final Collection<T> buffer;

      BufferBlock()
      {
         this(DEFAULT_BUFFER_CAPACITY);
      }

      BufferBlock(int capacity)
      {
         this.capacity = capacity;
         buffer = new ArrayList<>(capacity);
      }

      @Override
      public void apply(T t)
      {
         buffer.add(t);
         if (buffer.size() >= capacity)
            flush();
      }

      void flush()
      {
         if (!buffer.isEmpty())
         {
            flush(buffer);
            buffer.clear();
         }
      }

      protected abstract void flush(Iterable<T> buffer);
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
