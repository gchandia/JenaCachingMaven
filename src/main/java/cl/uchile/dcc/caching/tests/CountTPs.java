package cl.uchile.dcc.caching.tests;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;

public class CountTPs {
  public static void main(String[] args) {
    String s = "SELECT  ?var1 ?var2 ?var3 ?var4\r\n"
        + "WHERE\r\n"
        + "  { { ?var2  <http://www.wikidata.org/prop/direct/P2678>  ?var1 ;\r\n"
        + "             ?p ?o .\r\n"
        + "      ?var3  <http://www.wikidata.org/prop/direct/P2678>  ?var4\r\n"
        + "    }\r\n"
        + "  }";
    
    Query q = QueryFactory.create(s);
    
  }
}
