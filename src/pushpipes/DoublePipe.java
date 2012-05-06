package pushpipes;

import java.util.NoSuchElementException;
import java.util.functions.DoubleBinaryOperator;

/**
 * @author peter.levart@gmail.com
 */
public abstract class DoublePipe extends AbstractPipe
{
   protected DoubleBlock downstream;

   protected final DoubleBlock connect(DoubleBlock downstream)
   {
      if (this.downstream != null)
         throw new IllegalStateException("This DoublePipe is already connected to a downstream DoubleBlock");

      this.downstream = downstream;

      return downstream;
   }

   protected final void disconnect(Object downstream)
   {
      if (this.downstream != downstream)
         throw new IllegalStateException("This DoublePipe is not connected to the downstream DoubleBlock");

      this.downstream = null;
   }


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
