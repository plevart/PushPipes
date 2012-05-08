package pushpipes;

/**
 * @author peter.levart@gmail.com
 */
abstract class AbstractPipe<B>
{
   protected static final int DEFAULT_BUFFER_CAPACITY = 256;

   //
   // initiating execution

   protected abstract void process();

   //
   // connecting/disconnecting downstream

   protected B downstream;

   protected final AbstractPipe<B> connect(B downstream)
   {
      if (this.downstream != null)
         throw new IllegalStateException("This Pipe is already connected to a downstream");

      this.downstream = downstream;

      return this;
   }

   protected final void disconnect(B downstream)
   {
      if (this.downstream != downstream)
         throw new IllegalStateException("This Pipe is not connected to the downstream");

      this.downstream = null;
   }
}
