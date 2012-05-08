package pushpipes;

import java.util.*;
import java.util.functions.*;

/**
 * @author peter.levart@gmail.com
 */
public abstract class DoublePipe extends AbstractPipe<DoubleBlock>
{
   //
   // pipe chain building...

   //
   // pipe chain executing

   public double reduce(DoubleBinaryOperator reducer)
   {
      return process(new DoubleReducerBlock(reducer));
   }

   public double reduce(double base, DoubleBinaryOperator reducer)
   {
      return process(new DoubleReducerBlock(base, reducer));
   }

   private double process(DoubleResultBlock resultBlock)
   {
      AbstractPipe<DoubleBlock> pipe = connect(resultBlock);
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

   protected abstract static class DoubleResultBlock implements DoubleBlock
   {
      protected boolean hasResult;
      protected double result;

      DoubleResultBlock()
      {
      }

      DoubleResultBlock(double result)
      {
         this.hasResult = true;
         this.result = result;
      }

      double getResult()
      {
         if (!hasResult)
            throw new NoSuchElementException("No result");

         return result;
      }
   }

   protected static class DoubleReducerBlock extends DoubleResultBlock
   {
      private final DoubleBinaryOperator reducer;

      @SuppressWarnings("unchecked")
      DoubleReducerBlock(DoubleBinaryOperator reducer)
      {
         super();
         this.reducer = reducer;
      }

      DoubleReducerBlock(double base, DoubleBinaryOperator reducer)
      {
         super(base);
         this.reducer = reducer;
      }

      public void apply(double value)
      {
         if (hasResult)
         {
            result = reducer.eval(result, value);
         }
         else
         {
            result = value;
            hasResult = true;
         }
      }
   }
}
