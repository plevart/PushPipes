package pushpipes.v2;

import java.util.functions.*;

/**
 * @author peter
 * @created 5/9/12 @ 12:57 PM
 */
public abstract class ProducerBlock<T> implements Producer, Block<T>
{

   public static abstract class Tail<T> extends ProducerBlock<T>
   {
      @Override
      public boolean produceNext()
      {
         return false;
      }
   }

   public static abstract class Unbuffered<T> extends ProducerBlock<T>
   {
      private final Producer downstream;

      protected Unbuffered(Producer downstream)
      {
         this.downstream = downstream;
      }

      @Override
      public final boolean produceNext()
      {
         return downstream.produceNext();
      }
   }
}
