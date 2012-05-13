package pushpipes.v2;

import java.util.functions.LongBinaryOperator;

/**
 * @author peter.levart@gmail.com
 */
public interface LongTransformer extends LongConsumer, Producer
{
   boolean canConsume();

   @Override
   void consume(long value) throws IllegalStateException;

   //
   // Reducer

   final class ReducerTail implements LongTransformer
   {
      private final LongBinaryOperator reducer;
      private long result;

      public ReducerTail(LongBinaryOperator reducer, long base)
      {
         this.reducer = reducer;
         this.result = base;
      }

      @Override
      public boolean canConsume()
      {
         return true;
      }

      @Override
      public void consume(long value) throws IllegalStateException
      {
         result = reducer.eval(result, value);
      }

      @Override
      public boolean produce()
      {
         return false;
      }

      public long getResult()
      {
         return result;
      }
   }
}
