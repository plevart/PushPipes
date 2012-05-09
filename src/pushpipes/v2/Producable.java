package pushpipes.v2;

/**
 * @author peter
 * @created 5/9/12 @ 9:37 AM
 */
public interface Producable<DS extends Producer>
{
   Producer producer(DS downstream);
}
