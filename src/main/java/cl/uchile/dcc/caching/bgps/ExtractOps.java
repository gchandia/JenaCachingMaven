package cl.uchile.dcc.caching.bgps;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.Op1;
import org.apache.jena.sparql.algebra.op.Op2;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpN;
import org.apache.jena.sparql.algebra.op.OpPath;

import cl.uchile.dcc.caching.common_joins.Parser;

public class ExtractOps {
	
	public static ArrayList<Op> getOps(Op op){
		ArrayList<Op> ops = new ArrayList<Op>();
		getOps(op, ops);
		return ops;
	}
	
	public static void getOps(Op op, ArrayList<Op> ops){
		if(op instanceof OpBGP) {
			
		}else if (op instanceof OpPath) {
			
		} else if(op instanceof Op1) {
			ops.add(op);
			getOps(((Op1)op).getSubOp(),ops);
		} else if(op instanceof Op2) {
			ops.add(op);
			getOps(((Op2)op).getLeft(),ops);
			getOps(((Op2)op).getRight(),ops);
		} else if(op instanceof OpN) {
			ops.add(op);
			OpN opn = (OpN) op;
			for(Op sop:opn.getElements()) {
				getOps(sop, ops);
			}
		}
	}
	
	public static void replaceBgp(Op op, OpBGP target, OpBGP rep) {
		if(op instanceof OpBGP) {
			mutateBgp((OpBGP) op, rep);
		} else if(op instanceof Op1) {
			replaceBgp(((Op1)op).getSubOp(), target, rep);
		} else if(op instanceof Op2) {
			replaceBgp(((Op2)op).getLeft(), target, rep);
			if (((Op2)op).getRight() instanceof OpBGP) {
				OpBGP b = (OpBGP) ((Op2) op).getRight();
				b = rep;
			}
			replaceBgp(((Op2)op).getRight(), target, rep);
		} else if(op instanceof OpN) {
			OpN opn = (OpN) op;
			for(Op sop:opn.getElements()) {
				replaceBgp(sop, target, rep);
			}
		}
	}
	
	public static void mutateBgp(OpBGP target, OpBGP rep) {
		target.getPattern();
	}
	
	public static void main(String[] args) throws UnsupportedEncodingException {
		String query6 = "SELECT  ?v0 ?v1 ?v2 ?v3 ?v4\n"
				+ "WHERE\n"
				+ "  {   {   {   { ?v0  <http://dbpedia.org/ontology/iataLocationIdentifier>  ?v4 ;\n"
				+ "                     <http://dbpedia.org/ontology/location>  ?v2 ;\n"
				+ "                     a                     <http://dbpedia.org/ontology/Airport> .\n"
				+ "                ?v2  a                     <http://dbpedia.org/ontology/Settlement> ;\n"
				+ "                     <http://www.w3.org/2000/01/rdf-schema#label>  \"Crailsheim@de\"\n"
				+ "              }\n"
				+ "            UNION\n"
				+ "              { ?v0  <http://dbpedia.org/ontology/city>  ?v2 ;\n"
				+ "                     <http://dbpedia.org/property/iata>  ?v4 ;\n"
				+ "                     a                     <http://dbpedia.org/ontology/Airport> .\n"
				+ "                ?v2  a                     <http://dbpedia.org/ontology/Settlement> ;\n"
				+ "                     <http://www.w3.org/2000/01/rdf-schema#label>  \"Crailsheim@de\"\n"
				+ "              }\n"
				+ "          }\n"
				+ "        UNION\n"
				+ "          { ?v0  <http://dbpedia.org/ontology/city>  ?v2 ;\n"
				+ "                 <http://dbpedia.org/ontology/iataLocationIdentifier>  ?v4 ;\n"
				+ "                 a                     <http://dbpedia.org/ontology/Airport> .\n"
				+ "            ?v2  a                     <http://dbpedia.org/ontology/Settlement> ;\n"
				+ "                 <http://www.w3.org/2000/01/rdf-schema#label>  \"Crailsheim@de\"\n"
				+ "          }\n"
				+ "      }\n"
				+ "    UNION\n"
				+ "      { ?v0  <http://dbpedia.org/ontology/location>  ?v2 ;\n"
				+ "             <http://dbpedia.org/property/iata>  ?v4 ;\n"
				+ "             a                     <http://dbpedia.org/ontology/Airport> .\n"
				+ "        ?v2  a                     <http://dbpedia.org/ontology/Settlement> ;\n"
				+ "             <http://www.w3.org/2000/01/rdf-schema#label>  \"Crailsheim@de\"\n"
				+ "      }\n"
				+ "    OPTIONAL\n"
				+ "      { ?v0  <http://xmlns.com/foaf/0.1/homepage>  ?v1 }\n"
				+ "    OPTIONAL\n"
				+ "      { ?v0  <http://dbpedia.org/property/nativename>  ?v3 }\n"
				+ "  }";
		
		Parser p = new Parser();
		Query q6 = p.parseDbPedia(query6);
		ArrayList<Op> myOps = getOps(Algebra.compile(q6));
		//System.out.println(myOps);
		Op op = Algebra.compile(q6);
		op = ((Op1) op).getSubOp();
		Op op2 = ((Op2) op).getLeft();
		Op op3 = ((Op2) op2).getLeft();
		Op op4 = ((Op2) op3).getRight();
		System.out.println(op4);
		TransformCopy tc = new TransformCopy();
		Op op5 = tc.transform((OpBGP) op4);
		System.out.println(op5);
	}
}