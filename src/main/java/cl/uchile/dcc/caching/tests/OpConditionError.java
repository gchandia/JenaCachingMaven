package cl.uchile.dcc.caching.tests;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import cl.uchile.dcc.caching.common_joins.Parser;

public class OpConditionError {
  public static void main(String[] args) {
    String s = "SELECT  ?var1 ?var1Label ?var2 ?var3 ?var3Label ?var4\r\n"
        + "WHERE\r\n"
        + "  { ?var1  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q11173> ;\r\n"
        + "           <http://www.wikidata.org/prop/P1117>  ?var5 .\r\n"
        + "    ?var5  rdf:type              <http://wikiba.se/ontology#BestRank> ;\r\n"
        + "           <http://www.wikidata.org/prop/statement/P1117>  ?var2\r\n"
        + "    OPTIONAL\r\n"
        + "      { ?var5 <http://www.w3.org/ns/prov#wasDerivedFrom>/<http://www.wikidata.org/prop/reference/P248> ?var3\r\n"
        + "        OPTIONAL\r\n"
        + "          { ?var3  <http://www.wikidata.org/prop/direct/P356>  ?var4 }\r\n"
        + "      }\r\n"
        + "    SERVICE <http://wikiba.se/ontology#label>\r\n"
        + "      { <http://www.bigdata.com/rdf#serviceParam>\r\n"
        + "                  <http://wikiba.se/ontology#language>  \"en\"\r\n"
        + "      }\r\n"
        + "  }";
    
    Parser parser = new Parser();
    Query q = null;
    try {
      q = parser.parseDbPedia(s);
    } catch (Exception e) {}
    Op op = Algebra.compile(q);
    System.out.println(op);
    
  }
}
