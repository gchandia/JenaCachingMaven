package cl.uchile.dcc.caching.queries;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.tdb.TDBFactory;

public class ExperimentTwo {
	public static void main(String[] args) {
		String s1 = "SELECT ?var 1 WHERE {?var2 <http://www.wikidata.org/prop/direct/P40>  <http://www.wikidata.org/entity/Q76>; \n"
				+ "?var1 <http://www.wikidata.org/entity/Q186443532> }";
		String s2 = "SELECT ?var1 WHERE { <http://www.wikidata.org/entity/Q501128> ?var1  ?var2}";
		String s3 = "SELECT  * WHERE { ?var1  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q6256> ; "
				+ "<http://www.wikidata.org/prop/direct/P1566>  \"3175395\" }";
		String s4 = "SELECT  * WHERE { ?var1  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q6256> ;"
				+ " <http://www.wikidata.org/prop/direct/P1566>  \"2589581\" }";
		String s5 = "SELECT  ?var1 ?var1Label ?var2 ?var2Label WHERE { ?var1  <http://www.wikidata.org/prop/direct/P586>  ?var3 }";
		String s6 = "SELECT DISTINCT  ?var1 WHERE { ?var1  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q571> ; "
				+ "<http://www.wikidata.org/prop/direct/P364>  ?var2 ; <http://www.wikidata.org/prop/direct/P407>  ?var3 FILTER ( ?var2 != ?var3 )}";
		String s7 = "SELECT  ?var1 ?var2 WHERE { <http://www.wikidata.org/entity/Q25287> <http://www.wikidata.org/prop/direct/P625>  ?var1 "
				+ "OPTIONAL { <http://www.wikidata.org/entity/Q25287> <http://www.wikidata.org/prop/direct/P131>  ?var3 . "
				+ "?var3 <http://www.wikidata.org/prop/direct/P625>  ?var2}}";
		String s8 = "SELECT  ?var1 ?var2 WHERE { <http://www.wikidata.org/entity/Q417743> ?var1  ?var2}";
		String s9 = "SELECT  ?var1 ?var2 WHERE { <http://www.wikidata.org/entity/Q416426> ?var1  ?var2}";
		String s10 = "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P31>  ?y .\n"
				+ "    ?x  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q3294251>\n"
				+ "  }";
		String s11 = "SELECT  *\n"
				+ "WHERE\n"
				+ "  { ?x  <http://www.wikidata.org/prop/direct/P495>  ?y .\n"
				+ "    ?x  <http://www.wikidata.org/prop/direct/P495>  <http://www.wikidata.org/entity/Q15180>\n"
				+ "  }";
		
		Query q1 = QueryFactory.create(s11);
		String myModel = "D:\\tmp\\WikiDB";
		Dataset ds = TDBFactory.createDataset(myModel);
		// Write if I wanna write, but I'll be using read to query over it mostly
		ds.begin(ReadWrite.READ);
		
		// Define model and Query
		final Model model = ds.getDefaultModel();
		QueryExecution q11Exec = QueryExecutionFactory.create(q1, model);
		System.out.println("Executing select");
		ResultSet q11Results = q11Exec.execSelect();
		Op op = Algebra.compile(q1);
		op = Algebra.optimize(op);
		System.out.println("Executing exec");
		QueryIterator cache_qit = Algebra.exec(op, model);
		int resultAmount = 0;
		int cacheResultAmount = 0;
		
		System.out.println("Reading select");
		while (q11Results.hasNext()) {
			q11Results.next();
			resultAmount++;
			System.out.println(resultAmount);
		}
		
		System.out.println("Reading exec");
		while (cache_qit.hasNext()) {
			cache_qit.next();
			cacheResultAmount++;
			System.out.println(cacheResultAmount);
		}
		
		System.out.println(resultAmount);
		System.out.println(cacheResultAmount);
	}
}
