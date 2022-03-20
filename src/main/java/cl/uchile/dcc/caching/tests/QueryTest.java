package cl.uchile.dcc.caching.tests;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryType;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.tdb.TDBFactory;

import cl.uchile.dcc.blabel.label.GraphColouring.HashCollisionException;
import cl.uchile.dcc.caching.common_joins.Parser;
import cl.uchile.dcc.qcan.main.SingleQuery;

public class QueryTest {
	private static String myModel = "D:\\tmp\\WikiDB";
	  private static Dataset ds = TDBFactory.createDataset(myModel);
	
	public static void main(String[] args) {
		String s = "SELECT  ?var1\r\n"
				+ "WHERE\r\n"
				+ "  { ?var1  <http://www.wikidata.org/prop/direct/P345>  \"tt0160330\" }";
		
		String s2 = "SELECT  ?var1 ?var1Label ?var2 ?var3 ?var3Label ?var4\r\n"
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
		
		Parser p = new Parser();
		Query q = null;
		try {
			q = p.parseDbPedia(s);
		} catch (Exception e) {}
		ds.begin(ReadWrite.READ);
		final Model model = ds.getDefaultModel();
		Op alg = Algebra.compile(q);
		alg = Algebra.optimize(alg);
		QueryIterator qit = Algebra.exec(alg, model);
		int resultAmount = 0;
		
		while (qit.hasNext()) {
			qit.next();
			resultAmount++;
		}
		
		SingleQuery sq = null;
		QueryType qt;
		try {
			sq = new SingleQuery(q.toString(), true, true, false, true);
		} catch (InterruptedException | HashCollisionException e) {}
		
		System.out.println(sq.getQuery());
		System.out.println(resultAmount);
	}
}
