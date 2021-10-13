package cl.uchile.dcc.caching.bgps;

import java.util.ArrayList;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.BasicPattern;

public class ManipulateBgps {
	
	public static void removeBgps(ArrayList<Op> input, ArrayList<OpBGP> bgps) {
		for (OpBGP rem : bgps) {
			input.remove(rem);
		}
	}
	
	/*
	 * Transforms a list of different bgps into one bgp with n triples
	 */
	public static ArrayList<OpBGP> collapseBgps(ArrayList<ArrayList<OpBGP>> input) {
		ArrayList<OpBGP> output = new ArrayList<OpBGP>();
		BasicPattern bp = new BasicPattern();
		
		for (ArrayList<OpBGP> l : input) {
			for (OpBGP bgp : l) {
				bp.add(bgp.getPattern().get(0));
			}
		}
		
		OpBGP finalBGP = new OpBGP(bp);
		output.add(finalBGP);
		return output;
	}
	
	public static boolean checkIfInBgp(ArrayList<Op> input, ArrayList<OpBGP> check) {
		boolean flag = true;
		
		for (OpBGP bgp : check) {
			if (!input.contains(bgp)) flag = false;
		}
		
		return flag;
	}

}