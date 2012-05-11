package pushpipes.v2.test;

import pushpipes.v2.*;

import java.util.*;

/**
 * @author peter.levart@gmail.com
 */
public class SimpleTest
{
   public static void main(String[] args)
   {
      Producable<String> strings = Producable.from("abc", "bcd", "cde", "def", "efg");

      for (
         String s
         :
         strings
            .map(str -> str.toUpperCase())
            .sorted(Comparators.reverseOrder())
            .filter(str -> str.contains("D"))
            .map(str -> str.toLowerCase())
            .flatMap(str -> str.splitAsStream(""))
            .filter(str -> !str.isEmpty())
            .uniqueElements()
            .sorted(Comparators.<String>naturalOrder())
         )
         System.out.println(s);
   }
}
