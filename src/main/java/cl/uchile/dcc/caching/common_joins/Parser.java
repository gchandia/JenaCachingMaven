package cl.uchile.dcc.caching.common_joins;

import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;

public class Parser {
	static final String prefix = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
			"PREFIX abc: <http://www.metadata.net/harmony/ABCSchemaV5Commented.rdf#>" +
			"PREFIX bif: <http://www.openlinksw.com/schemas/bif#>" +
			"PREFIX category: <http://dbpedia.org/resource/Category:>" +
			"PREFIX co: <http://purl.org/ontology/co/core#>" +
			"PREFIX db: <http://dbpedia.org/> " +
			"PREFIX dc: <http://purl.org/dc/elements/1.1/>" +
			"PREFIX dbo: <http://dbpedia.org/ontology/>" +
			"PREFIX dbp: <http://dbpedia.org/property/>" +
			"PREFIX dbpo: <http://dbpedia.org/ontology/>" +
			"PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#> " +
			"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
			"PREFIX yago: <http://www.w3.org/2000/01/rdf-schema#> " +
			"PREFIX dbpedia-owl: <http://dbpedia.org/ontology/> " +
			"PREFIX dbpedia: <http://dbpedia.org/resource/>" +
			"PREFIX dbpedia2: <http://dbpedia.org/property/> " +
			"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> " +
			"PREFIX geo: <http://www.opengis.net/ont/geosparql#> " +
			"PREFIX gn: <http://www.geonames.org/ontology#>" +
			"PREFIX foaf: <http://xmlns.com/foaf/0.1/> " +
			"PREFIX mo: <http://purl.org/ontology/mo/>" +
			"PREFIX owl: <http://www.w3.org/2002/07/owl#>" +
			"PREFIX dbprop: <http://dbpedia.org/property/>" +
			"PREFIX dbpprop: <http://dbpedia.org/property/>" +
			"PREFIX prop: <http://dbpedia.org/property/>" +
			"PREFIX dbpr: <http://dbpedia.org/resource/>" +
			"PREFIX sesame: <http://www.openrdf.org/schema/sesame#>" +
			"PREFIX sioc: <http://rdfs.org/sioc/ns#>" +
			"PREFIX sioct: <http://rdfs.org/sioc/types#>" +
			"PREFIX skos: <http://www.w3.org/2004/02/skos/core#>" +
			"PREFIX type: <http://info.deepcarbon.net/schema/type#>" +
			"PREFIX arco: <http://www.gate.ac.uk/ns/ontologies/arcomem-data-model.owl#>" +
			System.getProperty("line.separator");
	
	public Query parseDbPedia(String text) throws Exception {
		String[] line = text.split("\t");
		String query = line[0];
		String spQuery = String.join(" ", query.split("\\+"));
		spQuery = spQuery.replaceAll("%(?![0-9a-fA-F]{2})", "%25");
		spQuery = spQuery.replaceAll("\\+", "%2B");
		String decQuery = URLDecoder.decode(spQuery, "UTF-8");
		
		Pattern qPattern = Pattern.compile("SELECT .*}+", Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);
		Pattern qPatternTwo = Pattern.compile("SELECT .*}+ LIMIT [0-9]+", Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);
		Matcher m = qPattern.matcher(decQuery);
		Matcher mTwo = qPatternTwo.matcher(decQuery);
		Query q;
		
		if (m.find()) {
			String myQuery = m.group(0);
			q = QueryFactory.create(prefix + myQuery, Syntax.syntaxARQ);
		} else if (mTwo.find()) {
			String myQuery = mTwo.group(0);
			q = QueryFactory.create(prefix + myQuery, Syntax.syntaxARQ);
		} else {
			q = null;
		}
		
		return q;
	}
	
	public static void main(String[] args) throws Exception {
		Parser p = new Parser();
		String s = "SELECT  ?var1 ?var2 ?var3 ?var4\r\n"
				+ "WHERE\r\n"
				+ "  { { ?var2  <http://www.wikidata.org/prop/direct/P2949>  ?var1 ;\r\n"
				+ "             <http://www.wikidata.org/prop/direct/P22>  ?var3 .\r\n"
				+ "      ?var3  <http://www.wikidata.org/prop/direct/P2949>  ?var4\r\n"
				+ "    }\r\n"
				+ "  }";
		Query q = p.parseDbPedia(s);
		System.out.println(q);
	}

}