package cl.uchile.dcc.caching.utils;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.syntax.ElementGroup;

import cl.uchile.dcc.blabel.label.GraphColouring.HashCollisionException;
import cl.uchile.dcc.caching.bgps.ExtractBgps;
import cl.uchile.dcc.caching.common_joins.Joins;
import cl.uchile.dcc.qcan.main.SingleQuery;

public class QueryBuilder {
	
	public static ArrayList<Query> buildQuery(ArrayList<ArrayList<OpBGP>> list) {
		ArrayList<Query> bgpQueries = new ArrayList<Query>();
		for (ArrayList<OpBGP> bgp : list) bgpQueries.addAll(buildQueryFromBGPs(bgp));
		return bgpQueries;
	}
	
	private static ArrayList<Query> buildQueryFromBGPs(ArrayList<OpBGP> bgps) {
		ArrayList<Query> newbgps = new ArrayList<Query>();
		Query q;
		ElementGroup elg;
		
		for (int i = 0; i < bgps.size(); i++) {
			q = QueryFactory.make();
			q.setQuerySelectType();
			q.setQueryResultStar(true);
			elg = new ElementGroup();
			Iterator<OpBGP> it = bgps.iterator();
			while (it.hasNext()) {
				OpBGP b = it.next();
				Triple t = b.getPattern().get(0);
				elg.addTriplePattern(t);
			}
			q.setQueryPattern(elg);
			newbgps.add(q);
		}
		return newbgps;
	}
	
	public static ArrayList<OpBGP> extractSizeOneTriples(ArrayList<OpBGP> input) {
		ArrayList<OpBGP> output = new ArrayList<OpBGP>();
		System.out.println(input.size());
		System.out.println(input.toString());
		OpBGP opb = input.get(0);
		
		for (int i = 0; i < opb.getPattern().size(); i++) {
			BasicPattern bp = new BasicPattern();
			bp.add(opb.getPattern().get(i));
			OpBGP b = new OpBGP(bp);
			output.add(b);
		}
		
		return output;
	}
	
	public static ArrayList<ArrayList<OpBGP>> reCanonicalise(ArrayList<ArrayList<OpBGP>> subBGPs) throws InterruptedException, HashCollisionException {
	    ArrayList<ArrayList<OpBGP>> output = new ArrayList<ArrayList<OpBGP>>();
	    
	    for (ArrayList<OpBGP> bgps : subBGPs) {
	      Query q = new Query();
	      SingleQuery sq;
	      ElementGroup elg;
	      
	      for (int i = 0; i < bgps.size(); i++) {
	        q = QueryFactory.make();
	        q.setQuerySelectType();
	        q.setQueryResultStar(true);
	        elg = new ElementGroup();
	        Iterator<OpBGP> it = bgps.iterator();
	        while (it.hasNext()) {
	          OpBGP b = it.next();
	          Triple t = b.getPattern().get(0);
	          elg.addTriplePattern(t);
	        }
	        q.setQueryPattern(elg);
	      }
	      sq = new SingleQuery(q.toString(), true, true, false, true);
	      q = QueryFactory.create(sq.getQuery(), Syntax.syntaxARQ);
	      ArrayList<OpBGP> b = ExtractBgps.getBgps(Algebra.compile(q));
	      Joins.cleanBGPs(b);
	      output.add(b);
	      System.out.println("Extracting query " + output.size() + " of " + subBGPs.size());
	    }
	    
	    return output;
	  }
	
	public static void main(String[] args) {
		
	}

}