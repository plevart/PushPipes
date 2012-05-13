package pushpipes.v2;

import java.util.functions.LongBinaryOperator;

/**
 * @author peter.levart@gmail.com
 */
public abstract class LongProducable
{
   public abstract Producer producer(LongTransformer downstream);

   //
   // execution

   public long reduce(long base, LongBinaryOperator reducerOp)
   {
      LongTransformer.ReducerTail reducer = new LongTransformer.ReducerTail(reducerOp, base);
      Producer producer = producer(reducer);
      while (producer.produce()) {}
      return reducer.getResult();
   }
}
