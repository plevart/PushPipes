package pushpipes;

import java.util.NoSuchElementException;
import java.util.functions.LongBinaryOperator;

/**
 * @author peter.levart@gmail.com
 */
public abstract class LongPipe extends AbstractPipe<LongBlock>
{
   //
   // pipe chain building...

   //
   // pipe chain executing

   public long reduce(LongBinaryOperator reducer)
   {
      return process(new LongReducerBlock(reducer));
   }

   public long reduce(long base, LongBinaryOperator reducer)
   {
      return process(new LongReducerBlock(base, reducer));
   }

   private long process(LongResultBlock resultBlock)
   {
      AbstractPipe<LongBlock> pipe = connect(resultBlock);
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

   protected abstract static class LongResultBlock implements LongBlock
   {
      protected boolean hasResult;
      protected long result;

      LongResultBlock()
      {
      }

      LongResultBlock(long result)
      {
         this.hasResult = true;
         this.result = result;
      }

      long getResult()
      {
         if (!hasResult)
            throw new NoSuchElementException("No result");

         return result;
      }
   }

   protected static class LongReducerBlock extends LongResultBlock
   {
      private final LongBinaryOperator reducer;

      @SuppressWarnings("unchecked")
      LongReducerBlock(LongBinaryOperator reducer)
      {
         super();
         this.reducer = reducer;
      }

      LongReducerBlock(long base, LongBinaryOperator reducer)
      {
         super(base);
         this.reducer = reducer;
      }

      public void apply(long value)
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
