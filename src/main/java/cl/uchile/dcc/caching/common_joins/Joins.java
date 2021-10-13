package cl.uchile.dcc.caching.common_joins;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.sparql.syntax.ElementVisitorBase;
import org.apache.jena.sparql.syntax.ElementWalker;

public class Joins {
	
	private boolean checkJoin(String[] a, String[] b) {
		return a[0].equals(b[0]) || a[0].equals(b[2]) || a[2].equals(b[0]) || a[2].equals(b[2]);
	}
	
	public static ArrayList<OpBGP> cleanBGPs(ArrayList<OpBGP> bgps) {
		ArrayList<OpBGP> clear = new ArrayList<OpBGP>();
		for (OpBGP bgp : bgps) {
			if (bgp.getPattern().size() == 1) { clear.add(bgp); }
			else {
				for (Triple t : bgp.getPattern()) {
					BasicPattern bp = new BasicPattern();
					bp.add(t);
					clear.add(new OpBGP(bp));
				}
			}
		}
		return clear;
	}
	
	private static boolean shareVar(Triple a, Triple b) {
		
		boolean flagVarSA = a.getSubject().isVariable();
		boolean flagVarPA = a.getPredicate().isVariable();
		boolean flagVarOA = a.getObject().isVariable();
		
		boolean flagVarSB = b.getSubject().isVariable();
		boolean flagVarPB = b.getPredicate().isVariable();
		boolean flagVarOB = b.getObject().isVariable();
		
		boolean sAsB = (flagVarSA && flagVarSB) && a.getSubject().equals(b.getSubject());
		boolean sApB = (flagVarSA && flagVarPB) && a.getSubject().equals(b.getPredicate());
		boolean sAoB = (flagVarSA && flagVarOB) && a.getSubject().equals(b.getObject());
		
		boolean pAsB = (flagVarPA && flagVarSB) && a.getPredicate().equals(b.getSubject());
		boolean pApB = (flagVarPA && flagVarPB) && a.getPredicate().equals(b.getPredicate());
		boolean pAoB = (flagVarPA && flagVarOB) && a.getPredicate().equals(b.getObject());
		
		boolean oAsB = (flagVarOA && flagVarSB) && a.getObject().equals(b.getSubject());
		boolean oApB = (flagVarOA && flagVarPB) && a.getObject().equals(b.getPredicate());
		boolean oAoB = (flagVarOA && flagVarOB) && a.getObject().equals(b.getObject());
		
		return (sAsB || sApB || sAoB || pAsB || pApB || pAoB || oAsB || oApB || oAoB);
	}
	
	private static boolean isConnectedBGP(ArrayList<OpBGP> input) {
		boolean flag = false;
		for (int i = 0; i < input.size(); i++) {
			OpBGP bgp = input.get(i);
			int j = 0;
			while (j < input.size()) {
				if (i == j) {
					j++;
					continue;
				}
				if (shareVar(bgp.getPattern().get(0), input.get(j).getPattern().get(0))) {
					flag = true;
					break;
				}
				j++;
			}
			if (!flag) return false;
			else flag = false;
		}
		return true;
	}
	
	private static ArrayList<ArrayList<OpBGP>> getConnectedBGPs(ArrayList<ArrayList<OpBGP>> input) {
		ArrayList<ArrayList<OpBGP>> cbgps = new ArrayList<ArrayList<OpBGP>>();
		for (ArrayList<OpBGP> l : input) {
			if (isConnectedBGP(l)  && l.size() > 1) cbgps.add(l);
		}
		return cbgps;
	}
	
	public ArrayList<TriplePath> getTriples(Query q) {
		final ArrayList<TriplePath> triples = new ArrayList<TriplePath>();

		ElementWalker.walk(q.getQueryPattern(), new ElementVisitorBase() {
			public void visit(ElementPathBlock e) {
				Iterator<TriplePath> t = e.patternElts();
				while (t.hasNext()) {
					triples.add(t.next());
				}
			}
		});
		
		return triples;
	}
	
	public Iterator<String> getJoins(String[] triples) {
		ArrayList<String> joins = new ArrayList<String>();
		for (int i = 0; i < triples.length - 1; i++) {
			for (int j = i + 1; j < triples.length; j++) {
				if (triples[i] != triples[j] &&
					checkJoin(triples[i].split("\t"), triples[j].split("\t"))) 
					joins.add(triples[i] + "\t" + triples[j]);
			}
		}
		return joins.iterator();
	}
	
	/**
	 * Generates all possible permutations of size k, k = 2,...,n with n being the amount of bgps, then checks if subbgps are connected, returning
	 * the ones that are
	 * @param bgps
	 * @return all subbgps from the initial query
	 */
	public static ArrayList<ArrayList<OpBGP>> getSubBGPs(ArrayList<OpBGP> bgps) {
		ArrayList<OpBGP> input = cleanBGPs(bgps);
		
		ArrayList<ArrayList<OpBGP>> subbgps = Subsets.getSubsets(input);
		
		ArrayList<ArrayList<OpBGP>> connectedSubBGPs = getConnectedBGPs(subbgps);
		
		return connectedSubBGPs;
	}
	
	public static ArrayList<ArrayList<OpBGP>> getSplitSubBGPs(ArrayList<ArrayList<OpBGP>> splitbgps) {
		ArrayList<ArrayList<OpBGP>> csSubBGPs = new ArrayList<ArrayList<OpBGP>>();
		
		for (ArrayList<OpBGP> bgps : splitbgps) csSubBGPs.addAll(getSubBGPs(bgps));
		
		return csSubBGPs;
	}
}