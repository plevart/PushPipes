package pushpipes.v2;

import java.util.functions.IntBinaryOperator;

/**
 * @author peter.levart@gmail.com
 */
public interface IntTransformer extends IntConsumer, Producer
{
   boolean canConsume();

   @Override
   void consume(int value) throws IllegalStateException;

   //
   // Reducer

   final class ReducerTail implements IntTransformer
   {
      private final IntBinaryOperator reducer;
      private int result;

      public ReducerTail(IntBinaryOperator reducer, int base)
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
      public void consume(int value) throws IllegalStateException
      {
         result = reducer.eval(result, value);
      }

      @Override
      public boolean produce()
      {
         return false;
      }

      public int getResult()
      {
         return result;
      }
   }
}
