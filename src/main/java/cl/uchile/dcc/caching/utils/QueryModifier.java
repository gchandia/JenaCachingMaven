package cl.uchile.dcc.caching.utils;

import java.util.ArrayList;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.BasicPattern;

/**
 * We use this class to manipulate a query's algebra as we wish
 * @author gch12
 *
 */
public class QueryModifier {
	
	private static void transformNode(ArrayList<ArrayList<OpBGP>> bgps, Node og, Node n) {
		for (ArrayList<OpBGP> l : bgps) {
			int s = l.size();
			for (int i = 0; i < s; i++) {
				Triple t = l.get(i).getPattern().get(0);
				if (t.getSubject().equals(og)) {
					BasicPattern bp = new BasicPattern();
					Triple nt = Triple.create(n, t.getPredicate(), t.getObject());
					bp.add(nt);
					l.set(i, new OpBGP(bp));
				}
				
				if (t.getObject().equals(og)) {
					BasicPattern bp = new BasicPattern();
					Triple nt = Triple.create(t.getSubject(), t.getPredicate(), n);
					bp.add(nt);
					l.set(i, new OpBGP(bp));
				}
			}
		}
	}
	
	public static void obPrTransformer(ArrayList<ArrayList<OpBGP>> bgps) {
		String v = "var";
		int n = 1;
		
		for (ArrayList<OpBGP> l : bgps) {
			for (OpBGP op : l) {
				Triple t = op.getPattern().get(0);
				if (!t.getSubject().isVariable()) transformNode(bgps, t.getSubject(), NodeFactory.createVariable(v + n++));
				if (!t.getObject().isVariable() && !t.getPredicate().toString().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) 
					transformNode(bgps, t.getObject(), NodeFactory.createVariable(v + n++));
			}
		}
	}
	
	public static void main(String[] args) {
		String v = "?var";
		int n = 1;
		System.out.println(v + ++n);
	}
}