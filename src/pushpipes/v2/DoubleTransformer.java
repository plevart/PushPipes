package pushpipes.v2;

import java.util.functions.DoubleBinaryOperator;

/**
 * @author peter.levart@gmail.com
 */
public interface DoubleTransformer extends DoubleConsumer, Producer
{
   boolean canConsume();

   @Override
   void consume(double value) throws IllegalStateException;

   //
   // Reducer

   final class ReducerTail implements DoubleTransformer
   {
      private final DoubleBinaryOperator reducer;
      private double result;

      public ReducerTail(DoubleBinaryOperator reducer, double base)
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
      public void consume(double value) throws IllegalStateException
      {
         result = reducer.eval(result, value);
      }

      @Override
      public boolean produce()
      {
         return false;
      }

      public double getResult()
      {
         return result;
      }
   }
}
