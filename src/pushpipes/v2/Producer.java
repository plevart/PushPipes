package pushpipes.v2;

/**
 * @author peter
 * @created 5/9/12 @ 10:06 AM
 */
public interface Producer
{
   /**
    * Tries to produce next output if it has any. This call does not necessarily produce any output
    * if there is none available.
    *
    * @return true if there is more output to produce or false if all output is exhausted
    */
   boolean produceNext();

   abstract class Stateless implements Producer
   {
      private final Producer downstream;

      public Stateless(Producer downstream)
      {
         this.downstream = downstream;
      }

      @Override
      public final boolean produceNext()
      {
         return downstream.produceNext();
      }
   }

   abstract class Stateful implements Producer
   {
      private final Producer downstream;

      public Stateful(Producer downstream)
      {
         this.downstream = downstream;
      }

      protected abstract boolean thisProduceNext();

      @Override
      public final boolean produceNext()
      {
         return downstream.produceNext() || thisProduceNext();
      }
   }

   abstract class Tail implements Producer
   {
      @Override
      public final boolean produceNext()
      {
         return false;
      }
   }
}
