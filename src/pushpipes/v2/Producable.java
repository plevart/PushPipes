package pushpipes.v2;

/**
 * @author peter.levart@gmail.com
 */
public interface Producable<DS extends Producer>
{
   Producer producer(DS downstream);
}
