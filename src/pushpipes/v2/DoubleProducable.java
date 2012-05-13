package pushpipes.v2;

import java.util.functions.DoubleBinaryOperator;

/**
 * @author peter.levart@gmail.com
 */
public abstract class DoubleProducable
{
   public abstract Producer producer(DoubleTransformer downstream);

   //
   // execution

   public double reduce(double base, DoubleBinaryOperator reducerOp)
   {
      DoubleTransformer.ReducerTail reducer = new DoubleTransformer.ReducerTail(reducerOp, base);
      Producer producer = producer(reducer);
      while (producer.produce()) {}
      return reducer.getResult();
   }
}
