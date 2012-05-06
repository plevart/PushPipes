package pushpipes;

import java.util.NoSuchElementException;
import java.util.functions.*;

/**
 * @author peter.levart@gmail.com
 */
public abstract class Pipe<T> extends AbstractPipe
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

   public static <T> Pipe<T> from(T... values)
   {
      return new ArrayPipe<>(values);
   }

   //
   // connecting/disconnecting downstream

   protected Block<? super T> downstream;

   protected final <DS extends Block<? super T>> DS connect(DS downstream)
   {
      if (this.downstream != null)
         throw new IllegalStateException("This Pipe is already connected to a downstream Block");

      this.downstream = downstream;

      return downstream;
   }

   protected final void disconnect(Block<? super T> downstream)
   {
      if (this.downstream != downstream)
         throw new IllegalStateException("This Pipe is not connected to the downstream Block");

      this.downstream = null;
   }

   //
   // pipe chain building...

   public Pipe<T> filter(final Predicate<? super T> predicate)
   {
      return connect(new Step<T, T>(this)
      {
         public void apply(T arg)
         {
            if (predicate.test(arg))
               downstream.apply(arg);
         }
      });
   }

   public <U> Pipe<U> map(final Mapper<? super T, ? extends U> mapper)
   {
      return connect(new Step<T, U>(this)
      {
         public void apply(T arg)
         {
            downstream.apply(mapper.map(arg));
         }
      });
   }

   public <U> Pipe<U> flatMap(final Mapper<? super T, ? extends Iterable<U>> mapper)
   {
      return connect(new Step<T, U>(this)
      {
         public void apply(T arg)
         {
            for (U element : mapper.map(arg))
               downstream.apply(element);
         }
      });
   }

   public IntPipe mapInt(final IntMapper<? super T> mapper)
   {
      return connect(new IntStep<T>(this)
      {
         public void apply(T arg)
         {
            downstream.apply(mapper.map(arg));
         }
      });
   }

   public LongPipe mapLong(final LongMapper<? super T> mapper)
   {
      return connect(new LongStep<T>(this)
      {
         public void apply(T arg)
         {
            downstream.apply(mapper.map(arg));
         }
      });
   }

   public DoublePipe mapDouble(final DoubleMapper<? super T> mapper)
   {
      return connect(new DoubleStep<T>(this)
      {
         public void apply(T arg)
         {
            downstream.apply(mapper.map(arg));
         }
      });
   }

   //
   // pipe chain executing

   public long count()
   {
      Counter counter = connect(new Counter());
      try
      {
         process();
         return counter.getCount();
      }
      finally
      {
         disconnect(counter);
      }
   }

   public void forEach(Block<? super T> block)
   {
      connect(block);
      try
      {
         process();
      }
      finally
      {
         disconnect(block);
      }
   }

   public T getFirst()
   {
      SingleResultBlock<T> collector = new SingleResultBlock<>(LongBreak.INSTANCE);
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
      return process(new SingleResultBlock<T>());
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
      connect(resultBlock);
      try
      {
         process();
         return resultBlock.getResult();
      }
      finally
      {
         disconnect(resultBlock);
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
      private final Pipe<I> upstream;

      Step(Pipe<I> upstream)
      {
         this.upstream = upstream;
      }

      @Override
      protected void process()
      {
         upstream.process();
      }
   }

   static abstract class IntStep<I> extends IntPipe implements Block<I>
   {
      private final Pipe<I> upstream;

      IntStep(Pipe<I> upstream)
      {
         this.upstream = upstream;
      }

      @Override
      protected void process()
      {
         upstream.process();
      }
   }

   static abstract class LongStep<I> extends LongPipe implements Block<I>
   {
      private final Pipe<I> upstream;

      LongStep(Pipe<I> upstream)
      {
         this.upstream = upstream;
      }

      @Override
      protected void process()
      {
         upstream.process();
      }
   }

   static abstract class DoubleStep<I> extends DoublePipe implements Block<I>
   {
      private final Pipe<I> upstream;

      DoubleStep(Pipe<I> upstream)
      {
         this.upstream = upstream;
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
      private final RuntimeException exceptionThrownOnSecondResult;

      SingleResultBlock()
      {
         this.exceptionThrownOnSecondResult = null;
      }

      SingleResultBlock(RuntimeException exceptionThrownOnSecondResult)
      {
         this.exceptionThrownOnSecondResult = exceptionThrownOnSecondResult;
      }

      public void apply(T t)
      {
         if (result != NONE)
            throw exceptionThrownOnSecondResult == null
                  ? new IllegalStateException("Multiple results")
                  : exceptionThrownOnSecondResult;

         result = t;
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

   static class LongBreak extends RuntimeException
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

//   public static void main(String[] args)
//   {
//      List<String> strings = Arrays.asList("Peter", "Renata", "XYZ");
//
//      Pipe
//         .from(strings)
//         .filter((s) -> s.length() > 3)
//         .map((s) -> s.toUpperCase())
//         .forEach((s) -> { System.out.println(s);})
//      ;
//
//      int len = Pipe.from(strings).mapInt((s) -> s.length()).reduce((l1, l2) -> l1 + l2);
//      System.out.println(len);
//   }
}
