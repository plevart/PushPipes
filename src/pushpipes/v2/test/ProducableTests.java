package pushpipes.v2.test;

import pushpipes.v2.*;

/**
 * @author peter.levart@gmail.com
 */
public class ProducableTests
{
   public static void main(String[] args)
   {
      SingleValuedProducable<String> strings = new ArrayProducable<>("abc", "def", "ghi");

      for (String s : strings.map(str -> str.toUpperCase()))
         System.out.println(s);
   }
}
