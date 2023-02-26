package cl.uchile.dcc.caching.tests;

import java.util.ArrayList;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.BasicPattern;

import cl.uchile.dcc.caching.bgps.ExtractBgps;
import cl.uchile.dcc.caching.common_joins.Parser;

public class SubQueryTest {
  
  ArrayList<ArrayList<OpBGP>> getSubQueries(ArrayList<ArrayList<OpBGP>> input) {
    int n = input.size();
    ArrayList<ArrayList<OpBGP>> output = new ArrayList<ArrayList<OpBGP>>();
    for (int i = 0; i < (1 << n); i++) {
      ArrayList<OpBGP> set = new ArrayList<OpBGP>();
      for (int j = 0; j < n; j++) {
	    if ((i & (1 << j)) > 0) {
	      set.add(input.get(j).get(0));
	    }
	  }
	  output.add(set);
	}
	return output;
  }
  
  static ArrayList<OpBGP> getSubQueriesV2(ArrayList<OpBGP> input) {
	int n = input.size();
	ArrayList<OpBGP> output = new ArrayList<OpBGP>();
	for (int x = 0; x < n; x++) {
	  int y = input.get(x).getPattern().size();
	  for (int i = 1; i < (1 << y); i++) {
		BasicPattern bp = new BasicPattern();
		for (int j = 0; j < y; j++) {
		  if ((i & (1 << j)) > 0) bp.add(input.get(x).getPattern().get(j));
		}
		output.add(new OpBGP(bp));
	  }
	}
	return output;
  }
  
  static ArrayList<OpBGP> getSubQueriesV4V1(ArrayList<OpBGP> input) {
		int n = input.size();
		ArrayList<OpBGP> output = new ArrayList<OpBGP>();
		for (int x = 0; x < n; x++) {
		  int y = input.get(x).getPattern().size();
		  for (int i = 0; i < y; i++) {
			  BasicPattern bp = new BasicPattern();
			  for (int j = 0; j < i; j++)
				bp.add(input.get(x).getPattern().get(j));
			  output.add(new OpBGP(bp));
		  }
		}
		return output;
	  }
  
  static ArrayList<OpBGP> getSubQueriesV4(ArrayList<OpBGP> input) {
		int n = input.size();
		ArrayList<OpBGP> output = new ArrayList<OpBGP>();
		for (int x = 0; x < n; x++) {
		  int y = input.get(x).getPattern().size();
		  BasicPattern bp = new BasicPattern();
		  for (int i = 0; i < y; i++) {
			bp.add(input.get(x).getPattern().get(i));
			BasicPattern bpCopy = new BasicPattern(bp);
			output.add(new OpBGP(bpCopy));
			System.out.println(output);
		  }
		}
		return output;
	  }
  
  public static void main(String[] args) throws Exception {
	String q = "SELECT ?a\r\n"
			+ "WHERE {\r\n"
			+ "   { ?a db:p ?b . ?b db:q ?c . ?c db:r ?a . }\r\n"
			+ "   UNION\r\n"
			+ "   { ?a db:s ?c . ?c db:t ?d . }\r\n"
			+ "}";
	
	Parser p = new Parser();
	Query qu = p.parseDbPedia(q);
	ArrayList<OpBGP> bgps = ExtractBgps.getBgps(Algebra.compile(qu));
	System.out.println(bgps);
	ArrayList<OpBGP> list = getSubQueriesV4(bgps);
	System.out.println(list);
  }
}
