package pushpipes.v2;

import java.util.functions.IntBinaryOperator;

/**
 * @author peter.levart@gmail.com
 */
public abstract class IntProducable
{
   public abstract Producer producer(IntTransformer downstream);

   //
   // execution

   public int reduce(int base, IntBinaryOperator reducerOp)
   {
      IntTransformer.ReducerTail reducer = new IntTransformer.ReducerTail(reducerOp, base);
      Producer producer = producer(reducer);
      while (producer.produce()) {}
      return reducer.getResult();
   }
}
