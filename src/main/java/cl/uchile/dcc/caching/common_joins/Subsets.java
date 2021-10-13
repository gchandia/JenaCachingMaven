package cl.uchile.dcc.caching.common_joins;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;

import cl.uchile.dcc.main.SingleQuery;
import cl.uchile.dcc.caching.bgps.ExtractBgps;

public class Subsets {
	private static OpBGP[] getSubset(ArrayList<OpBGP> input, int[] subset) {
		OpBGP[] result = new OpBGP[subset.length];
		for (int i = 0; i < subset.length; i++) result[i] = input.get(subset[i]);
	    return result;
	}
	
	private static ArrayList<OpBGP> convertToAL(OpBGP[] input) {
		ArrayList<OpBGP> output = new ArrayList<>();
		Collections.addAll(output, input);
		return output;
	}
	
	public static ArrayList<ArrayList<OpBGP>> getSubsets(ArrayList<OpBGP> input) {
		ArrayList<ArrayList<OpBGP>> subsets = new ArrayList<>();
		if (input.size() == 1) {
			subsets.add(input);
			return subsets;
		}
		for (int k = 2; k <= input.size(); k++) {
			int[] s = new int[k];
			
			if (k <= input.size()) {
			    for (int i = 0; (s[i] = i) < k - 1; i++);  
			    subsets.add(convertToAL(getSubset(input, s)));
			    for(;;) {
			        int i;
			        for (i = k - 1; i >= 0 && s[i] == input.size() - k + i; i--); 
			        if (i < 0) {
			            break;
			        }
			        s[i]++;
			        for (++i; i < k; i++) {
			            s[i] = s[i - 1] + 1; 
			        }
			        subsets.add(convertToAL(getSubset(input, s)));
			    }
			}
		}
		return subsets;
	}
	
	// generate actual subset by index sequence
	static int[] getSubset(int[] input, int[] subset) {
	    int[] result = new int[subset.length]; 
	    for (int i = 0; i < subset.length; i++) result[i] = input[subset[i]];
	    return result;
	}
	
	static void printList(int[] entry) {
		System.out.print("[");
		for (int i : entry) {
			System.out.print(i + ", ");
		}
		System.out.println("]");
	}
	
	static void subsets() {
		int[] input = {10, 20, 30, 40, 50};    // input array
		List<int[]> subsets = new ArrayList<>();
		
		for (int k = 2; k <= input.length; k++) {
			int[] s = new int[k];                  // here we'll keep indices 
			                                       // pointing to elements in input array

			if (k <= input.length) {
			    // first index sequence: 0, 1, 2, ...
			    for (int i = 0; (s[i] = i) < k - 1; i++);  
			    subsets.add(getSubset(input, s));
			    for(;;) {
			        int i;
			        // find position of item that can be incremented
			        for (i = k - 1; i >= 0 && s[i] == input.length - k + i; i--); 
			        if (i < 0) {
			            break;
			        }
			        s[i]++;                    // increment this item
			        for (++i; i < k; i++) {    // fill up remaining items
			            s[i] = s[i - 1] + 1; 
			        }
			        subsets.add(getSubset(input, s));
			    }
			}
		}
		for (int[] i : subsets) {
			printList(i);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Parser parser = new Parser();
		String s = "SELECT  ?v0\r\n"
				+ "WHERE\r\n"
				+ "  { ?v0  <http://dbpedia.org/ontology/iataLocationIdentifier>  ?v4 ;\r\n"
				+ "                     <http://dbpedia.org/ontology/location>  ?v2 ;\r\n"
				+ "                     a                     <http://dbpedia.org/ontology/Airport> .\r\n"
				+ "                ?v2  a                     <http://dbpedia.org/ontology/Settlement> ;\r\n"
				+ "                     <http://www.w3.org/2000/01/rdf-schema#label>  \"Crailsheim@de\"\r\n"
				+ "  }";
		Query q = parser.parseDbPedia(s);	// Parse line and turn into query q
		SingleQuery sq = new SingleQuery(q.toString(), true, true, false, true);
		q = QueryFactory.create(sq.getQuery(), Syntax.syntaxARQ);
		
		Op op = Algebra.compile(q);
		ArrayList<OpBGP> list = ExtractBgps.getBgps(op);
		ArrayList<OpBGP> list2 = Joins.cleanBGPs(list);
		System.out.println(list2);
		
		ArrayList<ArrayList<OpBGP>> out = getSubsets(list2);
		System.out.println(out.size());
		
		for (ArrayList<OpBGP> x : out) {
			System.out.println(x.toString());
		}
	}
}