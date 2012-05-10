package pushpipes.v2.test;

import java.util.*;

public class SimpleTest
{
   static class MyList<T> extends ArrayList<T>
   {
      @Override
      public Iterable<T> sorted(Comparator<? super T> comparator)
      {
         return super.sorted(comparator);
      }
   }

   public static void main(String[] args)
   {
      MyList<String> strings = new MyList<>();
      strings.add("aaa");
      strings.add("bbb");
      strings.add("ccc");

      System.out.println(strings.sorted(Comparators.reverseOrder()));
   }
}
