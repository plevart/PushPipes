package pushpipes.v2;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.functions.*;

/**
 * A {@link Transformer} is a {@link Consumer} and a {@link Producer}, but not necessarily both all the time.
 * The internal changeable state can disable transformer being a {@link Consumer} for indefinite number of invocations
 * of the {@link #produce()} method. Think of {@link #consume} as feeding input to the transformer and
 * {@link #produce} as generating it's output.<p/>
 * So if you have a bunch of objects to transform, you should feed them to the transformer as follows:<p/>
 * <pre>
 *    Transformer<T> transformer = ...;
 *
 *    for (T t : ...) {
 *       while (!transformer.canConsume() && transformer.produce()) {} // flush out any buffered output
 *       transformer.consume(t);
 *    }
 *
 *    while (transformer.produce()) {} // drain output
 * </pre>
 *
 * @author peter.levart@gmail.com
 */
public interface Transformer<T> extends Consumer<T>, Producer
{
   /**
    * @return true if this transformer is in a state that allows consuming input so that the
    *         next call to {@link #consume} will not throw {@link IllegalStateException}.
    */
   boolean canConsume();

   /**
    * @param t the object to consume
    * @throws IllegalStateException if this transformer is in a state that doesn't allow consuming
    */
   @Override
   void consume(T t) throws IllegalStateException;

   //
   // some tail implementations

   abstract class Tail<T> implements Transformer<T>
   {
      @Override
      public final boolean canConsume()
      {
         return true;
      }

      @Override
      public final boolean produce()
      {
         return false;
      }
   }

   final class ConsumerTail<T> extends Tail<T>
   {
      private final Consumer<? super T> consumer;

      public ConsumerTail(Consumer<? super T> consumer)
      {
         this.consumer = consumer;
      }

      @Override
      public void consume(T t) throws IllegalStateException
      {
         consumer.consume(t);
      }
   }

   final class BlockTail<T> extends Tail<T>
   {
      private final Block<? super T> block;

      public BlockTail(Block<? super T> block)
      {
         this.block = block;
      }

      @Override
      public void consume(T t) throws IllegalStateException
      {
         block.apply(t);
      }
   }

   final class ResultCounterTail extends Tail<Object>
   {
      private long count;

      @Override
      public void consume(Object o)
      {
         count++;
      }

      public long getResultCount(Producer producer)
      {
         while (producer.produce()) {}
         return count;
      }
   }

   final class IteratorTail<T> extends Tail<T> implements Iterator<T>
   {
      private T next;
      private boolean hasNext;
      private Producer producer;

      //
      // this is injected after construction as the chain's top-most Producer

      void setProducer(Producer producer)
      {
         this.producer = producer;
      }

      //
      // Consumer

      @Override
      public void consume(T t)
      {
         next = t;
         hasNext = true;
      }

      //
      // Iterator

      @Override
      public boolean hasNext()
      {
         while (!hasNext && producer.produce()) {}
         return hasNext;
      }

      @Override
      public T next()
      {
         if (!hasNext())
            throw new NoSuchElementException();

         try
         {
            return next;
         }
         finally
         {
            next = null;
            hasNext = false;
         }
      }

      @Override
      public void remove()
      {
         throw new UnsupportedOperationException();
      }
   }

   final class SingleResultTail<T> extends Tail<T>
   {
      private T result;
      private boolean hasResult;
      private Producer producer;

      //
      // this is injected after construction as the chain's top-most Producer

      void setProducer(Producer producer)
      {
         this.producer = producer;
      }

      //
      // Consumer

      @Override
      public void consume(T t)
      {
         if (hasResult)
            throw new IllegalStateException("Multiple results");

         result = t;
         hasResult = true;
      }

      //
      // single/first result retrieval

      public boolean hasResult()
      {
         while (!hasResult && producer.produce()) {}
         return hasResult;
      }

      public T getFirstResult() throws NoSuchElementException
      {
         if (hasResult())
            return result;
         else
            throw new NoSuchElementException();
      }

      public T getOnlyResult() throws NoSuchElementException, IllegalStateException
      {
         try
         {
            return getFirstResult();
         }
         finally
         {
            // this will throw IllegalStateException if there is a second result...
            while (producer.produce()) {}
         }
      }
   }

   final class FirstMatchTail<T> extends Tail<T>
   {
      private final Predicate<? super T> predicate;
      private boolean match;

      public FirstMatchTail(Predicate<? super T> predicate)
      {
         this.predicate = predicate;
      }

      //
      // ConsumerProducer

      @Override
      public void consume(T t)
      {
         match = predicate.test(t);
      }

      //
      // retrieval

      public boolean isMatch(Producer producer)
      {
         while (!match && producer.produce()) {}
         return match;
      }
   }

   final class ReducerTail<T> extends Tail<T>
   {
      private final BinaryOperator<T> reducer;

      public ReducerTail(BinaryOperator<T> reducer)
      {
         this.reducer = reducer;
      }

      public ReducerTail(T base, BinaryOperator<T> reducer)
      {
         this(reducer);
         result = base;
         hasResult = true;
      }

      private T result;
      private boolean hasResult;

      @Override
      public void consume(T t) throws IllegalStateException
      {
         result = hasResult ? reducer.eval(result, t) : t;
         hasResult = true;
      }

      public T getResult(Producer producer) throws NoSuchElementException
      {
         while (producer.produce()) {}

         if (hasResult)
            return result;

         throw new NoSuchElementException();
      }
   }

   final class IntReducerTail<T> extends Tail<T>
   {
      private final IntMapper<? super T> intMapper;
      private final IntBinaryOperator reducer;

      public IntReducerTail(IntMapper<? super T> intMapper, IntBinaryOperator reducer)
      {
         this.intMapper = intMapper;
         this.reducer = reducer;
      }

      public IntReducerTail(IntMapper<? super T> intMapper, int base, IntBinaryOperator reducer)
      {
         this(intMapper, reducer);
         result = base;
         hasResult = true;
      }

      private int result;
      private boolean hasResult;

      @Override
      public void consume(T t) throws IllegalStateException
      {
         result = hasResult ? reducer.eval(result, intMapper.map(t)) : intMapper.map(t);
         hasResult = true;
      }

      public int getResult(Producer producer) throws NoSuchElementException
      {
         while (producer.produce()) {}

         if (hasResult)
            return result;

         throw new NoSuchElementException();
      }
   }

   final class LongReducerTail<T> extends Tail<T>
   {
      private final LongMapper<? super T> longMapper;
      private final LongBinaryOperator reducer;

      public LongReducerTail(LongMapper<? super T> longMapper, LongBinaryOperator reducer)
      {
         this.longMapper = longMapper;
         this.reducer = reducer;
      }

      public LongReducerTail(LongMapper<? super T> longMapper, long base, LongBinaryOperator reducer)
      {
         this(longMapper, reducer);
         result = base;
         hasResult = true;
      }

      private long result;
      private boolean hasResult;

      @Override
      public void consume(T t) throws IllegalStateException
      {
         result = hasResult ? reducer.eval(result, longMapper.map(t)) : longMapper.map(t);
         hasResult = true;
      }

      public long getResult(Producer producer) throws NoSuchElementException
      {
         while (producer.produce()) {}

         if (hasResult)
            return result;

         throw new NoSuchElementException();
      }
   }

   final class DoubleReducerTail<T> extends Tail<T>
   {
      private final DoubleMapper<? super T> doubleMapper;
      private final DoubleBinaryOperator reducer;

      public DoubleReducerTail(DoubleMapper<? super T> doubleMapper, DoubleBinaryOperator reducer)
      {
         this.doubleMapper = doubleMapper;
         this.reducer = reducer;
      }

      public DoubleReducerTail(DoubleMapper<? super T> doubleMapper, double base, DoubleBinaryOperator reducer)
      {
         this(doubleMapper, reducer);
         result = base;
         hasResult = true;
      }

      private double result;
      private boolean hasResult;

      @Override
      public void consume(T t) throws IllegalStateException
      {
         result = hasResult ? reducer.eval(result, doubleMapper.map(t)) : doubleMapper.map(t);
         hasResult = true;
      }

      public double getResult(Producer producer) throws NoSuchElementException
      {
         while (producer.produce()) {}

         if (hasResult)
            return result;

         throw new NoSuchElementException();
      }
   }
}
