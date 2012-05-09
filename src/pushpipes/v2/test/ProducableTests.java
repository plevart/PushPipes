package pushpipes.v2.test;

import pushpipes.v2.*;

/**
 * @author peter
 * @created 5/9/12 @ 6:35 PM
 */
public class ProducableTests
{
   public static void main(String[] args)
   {
      SingleValuedProducable<String> strings = new ArrayProducable<String>("abc", "def", "ghi");

      for (String s : strings.map(str -> str.toUpperCase()))
         System.out.println(s);
   }
}
