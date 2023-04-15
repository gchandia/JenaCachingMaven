package cl.uchile.dcc.caching.bgps;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.Op1;
import org.apache.jena.sparql.algebra.op.Op2;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpN;
import org.apache.jena.sparql.algebra.op.OpPath;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.syntax.ElementGroup;

import cl.uchile.dcc.blabel.label.GraphColouring.HashCollisionException;
import cl.uchile.dcc.qcan.main.SingleQuery;


public class ExtractBgps {
	
	private static Map<String, String> varMap;
	
	public static Map<String, String> getVarMap() {
		return varMap;
	}
	
	public static String bytesToHex(byte[] hash) {
	  StringBuilder hexString = new StringBuilder(2 * hash.length);
	  for (int i = 0; i < hash.length; i++) {
	    String hex = Integer.toHexString(0xff & hash[i]);
	    if(hex.length() == 1) {
	      hexString.append('0');
	    }
	    hexString.append(hex);
	  }
	  return hexString.toString();
	}
	
	public static String hash256(String input) {
	  MessageDigest digest = null;
      try {
        digest = MessageDigest.getInstance("SHA-256");
      } catch (NoSuchAlgorithmException e) {}
      byte[] encodedhash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
      String hex = bytesToHex(encodedhash);
      return hex;
	}
	
	public static String encodePath(String path) {
	  String pred = "http://path.org/";
	  return pred + hash256(path);
	}
	
	public static ArrayList<OpBGP> getBgps(Op op) {
		ArrayList<OpBGP> bgps = new ArrayList<OpBGP>();
		getBgps(op,bgps);
		return bgps;
	}
	
	public static ArrayList<OpBGP> getSplitBgps(Op op) {
		ArrayList<OpBGP> splitBgps = new ArrayList<OpBGP>();
		getSplitBgps(op,splitBgps);
		return splitBgps;
	}
	
	public static void getBgps(Op op, ArrayList<OpBGP> bgps) {
		if(op instanceof OpBGP) {
			bgps.add((OpBGP)op);
		}else if (op instanceof OpPath) {
			TriplePath path = ((OpPath)op).getTriplePath();
			BasicPattern bp = new BasicPattern();
			//Triple nt = Triple.create(path.getSubject(), NodeFactory.createURI(path.getPath().toString()), path.getObject());
			Triple nt = Triple.create(path.getSubject(), 
			                          NodeFactory.createURI(encodePath(path.getPath().toString())), 
			                          path.getObject());
            bp.add(nt);
			bgps.add(new OpBGP(bp));
		} else if (op instanceof Op1) {
			getBgps(((Op1)op).getSubOp(),bgps);
		} else if (op instanceof Op2) {
			getBgps(((Op2)op).getLeft(),bgps);
			getBgps(((Op2)op).getRight(),bgps);
		} else if (op instanceof OpSequence) {
			OpN opn = (OpN) op;
			BasicPattern bp = new BasicPattern();
			for (Op sop:opn.getElements()) {
				if (sop instanceof OpPath) {
					TriplePath path = ((OpPath) sop).getTriplePath();
					Triple t = Triple.create(path.getSubject(), NodeFactory.createURI(encodePath(path.getPath().toString())), path.getObject());
					bp.add(t);
				} else {
				  getBgpsAux(bp, sop);
				}
			}
			bgps.add(new OpBGP(bp));
		} else if (op instanceof OpN) {
			OpN opn = (OpN) op;
			for (Op sop:opn.getElements()) {
				getBgps(sop,bgps);
			}
		}
	}
	
	public static void getBgpsAux(BasicPattern bp, Op op) {
	  if (op instanceof OpBGP) {
		OpBGP b = (OpBGP) op;
		Triple t = Triple.create(b.getPattern().get(0).getSubject(), b.getPattern().get(0).getPredicate(), b.getPattern().get(0).getObject());
		bp.add(t);
	  } else if (op instanceof Op1) {
		getBgpsAux(bp, ((Op1)op).getSubOp());
	  } else if (op instanceof Op2) {
		getBgpsAux(bp, ((Op2)op).getLeft());
		getBgpsAux(bp, ((Op2)op).getRight());
	  }
	}
	
	/**
	 * WILL COUNT PROPERTY PATHS AS A NORMAL TRIPLE
	 * @param op
	 * @param splitbgps
	 */
	public static void getSplitBgps(Op op, ArrayList<OpBGP> splitbgps) {
		if (op instanceof OpBGP) {
			splitbgps.add((OpBGP)op);
		} else if (op instanceof OpPath) {
			TriplePath path = ((OpPath)op).getTriplePath();
			BasicPattern bp = new BasicPattern();
			Triple nt = Triple.create(path.getSubject(), NodeFactory.createURI(path.getPath().toString()), path.getObject());
			bp.add(nt);
			splitbgps.add(new OpBGP(bp));
		} else if (op instanceof Op1) {
			getSplitBgps(((Op1)op).getSubOp(), splitbgps);
		} else if (op instanceof Op2) {
			getSplitBgps(((Op2)op).getLeft(), splitbgps);
			getSplitBgps(((Op2)op).getRight(), splitbgps);
		} else if(op instanceof OpN) {
			OpN opn = (OpN) op;
			for(Op sop : opn.getElements()) {
				getSplitBgps(sop, splitbgps);
			}
		}
	}
	
	public static void extractBGP(Query q) {
		Op op = Algebra.compile(q);
		ArrayList<OpBGP> opbgps = getBgps(op);
		System.out.println(opbgps.toString());
	}
	
	public static void extractSplitBGPs(Query q) {
		Op op = Algebra.compile(q);
		ArrayList<OpBGP> splitbgps = getSplitBgps(op);
		System.out.println(splitbgps.toString());
	}
	
	public static OpBGP unifyBGPs(ArrayList<OpBGP> input) {
		BasicPattern bp = new BasicPattern();
		
		for (OpBGP bgp : input) {
			bp.add(bgp.getPattern().get(0));
		}
		
		OpBGP output = new OpBGP(bp);
		return output;
	}
	
	public static OpBGP canonBGP(OpBGP input) throws Exception {
		Query q = QueryFactory.make();
		q.setQuerySelectType();
		q.setQueryResultStar(true);
		ElementGroup elg = new ElementGroup();
		for (int i = 0; i < input.getPattern().size(); i++) {
			elg.addTriplePattern(input.getPattern().get(i));
		}
		q.setQueryPattern(elg);
		SingleQuery sq = new SingleQuery(q.toString(), true, true, false, true);
		q = QueryFactory.create(sq.getQuery(), Syntax.syntaxARQ);
		Op op = Algebra.compile(q);
		ArrayList<OpBGP> bgps = getBgps(op);
		varMap = sq.getVarMap();
		return bgps.get(0);
	}
	
	/**
	 * Only separates bgps into chunks of size 1
	 * @param input
	 * @return
	 * @throws Exception
	 */
	public static ArrayList<OpBGP> separateBGPs(ArrayList<OpBGP> input) {
		ArrayList<OpBGP> output = new ArrayList<OpBGP>();
		
		for (int i = 0; i < input.size(); i++) {
			OpBGP bgp = input.get(i);
			for (int j = 0; j < bgp.getPattern().size(); j++) {
				Triple t = input.get(i).getPattern().get(j);
				Query q = QueryFactory.make();
				q.setQuerySelectType();
				q.setQueryResultStar(true);
				ElementGroup elg = new ElementGroup();
				elg.addTriplePattern(t);
				q.setQueryPattern(elg);
				output.addAll(getBgps(Algebra.compile(q)));
			}
		}
		
		return output;
	}
	
	/*
	 * Gets bgps and returns bgps of size one that have been canonicalised
	 */
	public static ArrayList<OpBGP> separateCanonBGPs(ArrayList<OpBGP> input) throws InterruptedException, HashCollisionException {
		ArrayList<OpBGP> output = new ArrayList<OpBGP>();
		
		for (int i = 0; i < input.size(); i++) {
			OpBGP bgp = input.get(i);
			for (int j = 0; j < bgp.getPattern().size(); j++) {
				Triple t = input.get(i).getPattern().get(j);
				Query q = QueryFactory.make();
				q.setQuerySelectType();
				q.setQueryResultStar(true);
				ElementGroup elg = new ElementGroup();
				elg.addTriplePattern(t);
				q.setQueryPattern(elg);
				SingleQuery sq = new SingleQuery(q.toString(), true, true, false, true);
				q = QueryFactory.create(sq.getQuery(), Syntax.syntaxARQ);
				output.addAll(getBgps(Algebra.compile(q)));
			}
		}
		
		return output;
	}
	
	public static void main(String[] args) {
	  /*String s = "SELECT DISTINCT  ?var1 "
	          + "WHERE"
	          + "{ BIND(<http://www.wikidata.org/entity/Q62155> AS ?var2)"
	          + "?var2 (<http://www.wikidata.org/prop/direct/P279>)* ?var1"
	          + "}";*/
	  String s2 = "PREFIX ex: <http://example.org/#>"
			  + "SELECT DISTINCT *"
			  + "WHERE"
			  + "{ex:Chile ex:borders* ?c ."
			  + "?c ex:name ?n ."
			  + "}";
	  Query q = QueryFactory.create(s2);
	  /*Op op = Algebra.compile(q);
	  Op op2 = ((Op1) op).getSubOp();
	  Op op3 = ((Op1) op2).getSubOp();
	  //Op op3L = ((Op2) op3).getLeft();
	  Op op3R = ((Op2) op3).getRight();
	  System.out.println(op3R);
	  TriplePath path = ((OpPath)op3R).getTriplePath();
	  System.out.println(path.getPath());
	  System.out.println(encodePath(path.getPath().toString()));
	  System.out.println(getBgps(op3R));*/
	  Op op = Algebra.compile(q);
	  System.out.println(getBgps(op));
	}
}
