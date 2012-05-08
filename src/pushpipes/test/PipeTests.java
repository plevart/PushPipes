package pushpipes.test;

import pushpipes.*;

import java.util.*;

/**
 * @author peter.levart@gmail.com
 */
public class PipeTests
{
   public static void main(String[] args)
   {
      String poem = "O Vrba! srečna, draga vas domača,\n" +
                    "kjer hiša mojega stoji očeta;\n" +
                    "de b' uka žeja me iz tvojga svéta\n" +
                    "speljala ne bila, goljfiva kača!\n" +
                    "\n" +
                    "Ne vedel bi, kako se v strup prebrača\n" +
                    "vse, kar srce si sladkega obeta;\n" +
                    "mi ne bila bi vera v sebe vzeta,\n" +
                    "ne bil viharjov nótranjih b' igrača!\n" +
                    "\n" +
                    "Zvestó srce in delavno ročico\n" +
                    "za doto, ki je nima miljonarka,\n" +
                    "bi bil dobil z izvoljeno devico;\n" +
                    "\n" +
                    "mi mirno plavala bi moja barka,\n" +
                    "pred ognjam dom, pred točo mi pšenico\n" +
                    "bi bližnji sosed vároval - svet' Marka.";

      System.out.println("\n" + poem + "\n\n                 France Prešeren\n---\n");

      List<String> words = Pipe.from(poem)
         .flatMap(s -> s.splitAsStream("[ ,\\.;!'\\-\\n]+"))
         .map(s -> s.toLowerCase(new Locale("sl_SI")))
         .into(new ArrayList<String>());

      int chars = Pipe.from(words).mapInt(s -> s.length()).reduce((l1, l2) -> l1 + l2);

      System.out.println("word stream (" + words.size() + " words, " + chars + " chars): " + words + "\n");

      System.out.println("distinct words by word lengths:\n");

      Pipe.from(words)
         .mapped(w -> w.length())
         .swap()
         .groupByKeyValuesInto(HashSet<String>::new)
         .sortBy((len, wrds) -> wrds.size())
         .forEach((len, wrds) ->
          {
             System.out.println("length=" + len + " : " + wrds.size() + " : " + wrds);
          });

      System.out.println("\ndistinct words by containing chars:\n");

      Pipe.from(words)
         .mapped(w -> w.splitAsStream(""))
         .flatMap((w, cs) -> cs)
         .filterValues(c -> !c.isEmpty())
         .swap()
         .groupByKeyValuesInto(HashSet<String>::new)
         .sortBy((c, wrds) -> wrds.size())
         .forEach((c, wrds) ->
          {
             System.out.println("char='" + c + "' : " + wrds.size() + " : " + wrds);
          });

      System.out.println();
   }
}
