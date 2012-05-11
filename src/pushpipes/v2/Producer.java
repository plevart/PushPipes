package pushpipes.v2;

/**
 * Base contract for producers - objects that produce some output, zero or one piece at each call to
 * {@link #produce} method, which also returns indication when the "ammunition" for production is exhausted.<p/>
 * {@link Producer} is usually obtained from {@link Producable#producer}.<p/>
 * <i>The analogy: Producer/Producable ~ Iterator/Iterable</i>
 *
 * @see Producable
 * @see Consumer
 * @see Transformer
 *
 * @author peter.levart@gmail.com
 */
public interface Producer
{
   /**
    * Try to produce one piece of output. One call to this method produces at most one piece of output,
    * but need not produce any output.
    *
    * @return true if this producer has more output to produce or false if it has exhausted all output
    */
   boolean produce();
}
