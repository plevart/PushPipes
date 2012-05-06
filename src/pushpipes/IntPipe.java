package pushpipes;

import java.util.NoSuchElementException;
import java.util.functions.IntBinaryOperator;

/**
 * @author peter.levart@gmail.com
 */
public abstract class IntPipe extends AbstractPipe
{
   protected IntBlock downstream;

   protected final IntBlock connect(IntBlock downstream)
   {
      if (this.downstream != null)
         throw new IllegalStateException("This IntPipe is already connected to a downstream IntBlock");

      this.downstream = downstream;

      return downstream;
   }

   protected final void disconnect(Object downstream)
   {
      if (this.downstream != downstream)
         throw new IllegalStateException("This IntPipe is not connected to the downstream IntBlock");

      this.downstream = null;
   }


   //
   // pipe chain building...

   //
   // pipe chain executing

   public int reduce(IntBinaryOperator reducer)
   {
      return process(new IntReducerBlock(reducer));
   }

   public int reduce(int base, IntBinaryOperator reducer)
   {
      return process(new IntReducerBlock(base, reducer));
   }

   private int process(IntResultBlock resultBlock)
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

   protected abstract static class IntResultBlock implements IntBlock
   {
      protected boolean hasResult;
      protected int result;

      IntResultBlock()
      {
      }

      IntResultBlock(int result)
      {
         this.hasResult = true;
         this.result = result;
      }

      int getResult()
      {
         if (!hasResult)
            throw new NoSuchElementException("No result");

         return result;
      }
   }

   protected static class IntReducerBlock extends IntResultBlock
   {
      private final IntBinaryOperator reducer;

      @SuppressWarnings("unchecked")
      IntReducerBlock(IntBinaryOperator reducer)
      {
         super();
         this.reducer = reducer;
      }

      IntReducerBlock(int base, IntBinaryOperator reducer)
      {
         super(base);
         this.reducer = reducer;
      }

      public void apply(int value)
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
