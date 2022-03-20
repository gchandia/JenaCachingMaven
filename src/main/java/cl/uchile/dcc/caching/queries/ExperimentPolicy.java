package cl.uchile.dcc.caching.queries;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.sparql.syntax.ElementVisitorBase;
import org.apache.jena.sparql.syntax.ElementWalker;
import org.apache.jena.tdb.TDBFactory;
import cl.uchile.dcc.caching.bgps.ExtractBgps;
import cl.uchile.dcc.caching.cache.SolutionCache;
import cl.uchile.dcc.caching.common_joins.Joins;
import cl.uchile.dcc.caching.common_joins.Parser;
import cl.uchile.dcc.qcan.main.SingleQuery;

public class ExperimentPolicy {
  private static SolutionCache myCache;
  private static ArrayList<Query> checkedSubQueries;
  private static ArrayList<OpBGP> checkedBgpSubQueries;
  private static ArrayList<Query> mySubQueries;
  private static ArrayList<OpBGP> myBgpSubQueries;
  private static ArrayList<Query> cachedSubQueries;
  private static ArrayList<OpBGP> cachedBgpSubQueries;
  private static String myModel = "C:\\Thesis\\WikiDB";
  private static Dataset ds = TDBFactory.createDataset(myModel);
  private static Model model;
  private static int totalTps = 0;
  private static int queryNumber = 1;

  public ExperimentPolicy() {
    checkedSubQueries = new ArrayList<Query>();
    checkedBgpSubQueries = new ArrayList<OpBGP>();
    mySubQueries = new ArrayList<Query>();
    myBgpSubQueries = new ArrayList<OpBGP>();
    cachedSubQueries = new ArrayList<Query>();
    cachedBgpSubQueries = new ArrayList<OpBGP>();
    myCache = new SolutionCache();
    ds.begin(ReadWrite.READ);
    model = ds.getDefaultModel();
  }
  
  static int countTriplePatterns(Query q) {
    totalTps = 0;
    ElementWalker.walk(q.getQueryPattern(),
        new ElementVisitorBase() {
            public void visit(ElementPathBlock el) {
                Iterator<TriplePath> triples = el.patternElts();
                while (triples.hasNext()) {
                    totalTps++;
                }
            }
        }
    );
    return totalTps;
  }
  
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
  
  Comparator<ArrayList<OpBGP>> subQueryComparator = new Comparator<ArrayList<OpBGP>>()
  {
      public int compare(ArrayList<OpBGP> o1, ArrayList<OpBGP> o2)
      {
          return Integer.compare(o2.size(), o1.size());
      }
  };
  
  private static boolean isConnectedBgp(ArrayList<OpBGP> input) {
    // For each bgp, check if it shares a var with any other bgp
    boolean flag = true;
    if (input.size() == 1) return true;
    
    for (OpBGP checkBgp : input) {
      boolean subflag = false;
      for (OpBGP checkedBgp : input) {
        if (checkBgp == checkedBgp) continue;
        if (Joins.shareVar(checkBgp.getPattern().get(0), checkedBgp.getPattern().get(0))) {
          subflag = true;
        }
      }
      if (!subflag) {
        flag = false;
        break;
      }
    }
    
    return flag;
  }
  
  ArrayList<ArrayList<OpBGP>> removeDisconnectedBgps(ArrayList<ArrayList<OpBGP>> input) {
    ArrayList<ArrayList<OpBGP>> output = new ArrayList<ArrayList<OpBGP>>();
    
    for (ArrayList<OpBGP> bgp : input) {
      if (isConnectedBgp(bgp)) {
        output.add(bgp);
      }
    }
    
    return output;
  }
  
  ArrayList<OpBGP> canonicaliseBgpList(ArrayList<ArrayList<OpBGP>> input) throws Exception {
    ArrayList<OpBGP> output = new ArrayList<OpBGP>();
    
    for (ArrayList<OpBGP> bgps : input) {
      Query q = new Query();
      SingleQuery sq;
      ElementGroup elg;
      
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
      
      sq = new SingleQuery(q.toString(), true, true, false, true);
      q = QueryFactory.create(sq.getQuery(), Syntax.syntaxARQ);
      ArrayList<OpBGP> newbgp = ExtractBgps.getBgps(Algebra.compile(q));
      output.addAll(newbgp);
    }
    
    return output;
  }
  
  ArrayList<Query> canonicaliseList(ArrayList<ArrayList<OpBGP>> input) throws Exception {
    ArrayList<Query> output = new ArrayList<Query>();
    
    for (ArrayList<OpBGP> bgps : input) {
      Query q = new Query();
      SingleQuery sq;
      ElementGroup elg;
      
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
      
      sq = new SingleQuery(q.toString(), true, true, false, true);
      q = QueryFactory.create(sq.getQuery(), Syntax.syntaxARQ);
      output.add(q);
    }
    
    return output;
  }
  
  private static boolean areTriplesEquivalent(Triple a, Triple b) {
    boolean flagVarSA = a.getSubject().isVariable();
    boolean flagVarPA = a.getPredicate().isVariable();
    boolean flagVarOA = a.getObject().isVariable();
    
    boolean flagVarSB = b.getSubject().isVariable();
    boolean flagVarPB = b.getPredicate().isVariable();
    boolean flagVarOB = b.getObject().isVariable();
    
    boolean sAsB = (flagVarSA && flagVarSB) || a.getSubject().equals(b.getSubject());
    boolean pApB = (flagVarPA && flagVarPB) || a.getPredicate().equals(b.getPredicate());
    boolean oAoB = (flagVarOA && flagVarOB) || a.getObject().equals(b.getObject());
    
    return (sAsB && pApB && oAoB);
  }
  
  private static boolean isSubQuery(Query one, Query two) {
    boolean flag = false;
    ArrayList<OpBGP> bgpOne = ExtractBgps.getBgps(Algebra.compile(one));
    ArrayList<OpBGP> bgpTwo = ExtractBgps.getBgps(Algebra.compile(two));
    
    for (OpBGP bgp1 : bgpOne) {
      boolean subflag = false;
      for (Triple t1 : bgp1.getPattern()) {
        for (OpBGP bgp2 : bgpTwo) {
          for (Triple t2 : bgp2.getPattern()) {
            if (areTriplesEquivalent(t1, t2)) subflag = true;
          }
        }
      }
      if (!subflag) return flag;
    }
    
    flag = true;
    return flag;
  }
  
  private static boolean isBgpSubQuery(OpBGP one, OpBGP two) {
    boolean flag = false;
    
    ArrayList<OpBGP> bgpOne = new ArrayList<OpBGP>();
    for (Triple t : one.getPattern()) {
      BasicPattern bp = new BasicPattern();
      bp.add(t);
      OpBGP b = new OpBGP(bp);
      bgpOne.add(b);
    }
    
    ArrayList<OpBGP> bgpTwo = new ArrayList<OpBGP>();
    for (Triple t : two.getPattern()) {
      BasicPattern bp = new BasicPattern();
      bp.add(t);
      OpBGP b = new OpBGP(bp);
      bgpTwo.add(b);
    }
    
    for (OpBGP bgp1 : bgpOne) {
      boolean subflag = false;
      for (Triple t1 : bgp1.getPattern()) {
        for (OpBGP bgp2 : bgpTwo) {
          for (Triple t2 : bgp2.getPattern()) {
            if (areTriplesEquivalent(t1, t2)) subflag = true;
          }
        }
      }
      if (!subflag) return flag;
    }
    
    flag = true;
    return flag;
  }
  
  private static boolean checkForSubQuery(Query q) {
    boolean flag = false;
    for (Query sq : cachedSubQueries) {
      if (isSubQuery(q, sq)) flag = true;
    }
    return flag;
  }
  
  private static boolean checkForBgpSubQuery(OpBGP bgp) {
    boolean flag = false;
    for (OpBGP b : cachedBgpSubQueries) {
      if (isBgpSubQuery(bgp, b)) flag = true;
    }
    return flag;
  }
  
  static void cacheQuery(Query q) {
    myCache.cacheConstants(q);
    ArrayList<OpBGP> qBgps = ExtractBgps.getBgps(Algebra.compile(q));
    QueryExecution q11Exec = QueryExecutionFactory.create(q, model);
    ResultSet q11Results = q11Exec.execSelect();
    myCache.cache(qBgps.get(0), q11Results);
  }
  
  static void cacheBgpQuery(OpBGP bgp) {
    ArrayList<OpBGP> bgps = new ArrayList<OpBGP>();
    for (Triple t : bgp.getPattern()) {
      BasicPattern bp = new BasicPattern();
      bp.add(t);
      OpBGP b = new OpBGP(bp);
      bgps.add(b);
    }
    
    Query q = new Query();
    ElementGroup elg;
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
    
    myCache.cacheConstants(q);
    ArrayList<OpBGP> qBgps = ExtractBgps.getBgps(Algebra.compile(q));
    QueryExecution qExec = QueryExecutionFactory.create(q, model);
    ResultSet qResults = qExec.execSelect();
    myCache.cache(qBgps.get(0), qResults);
  }
  
  static void checkQueue(Query q) {
    if (mySubQueries.isEmpty()) {
      checkedSubQueries.add(q);
      mySubQueries.add(q);
      return;
    }
    
    if (checkForSubQuery(q)) {
      checkedSubQueries.add(q);
      return;
    }
    
    for (Query qu : checkedSubQueries) {
      if (q.equals(qu)) return;
    }
    
    for (Query qu : mySubQueries) {
      if (q.equals(qu)){
        cacheQuery(q);
        checkedSubQueries.add(q);
        cachedSubQueries.add(q);
        mySubQueries.remove(qu);
        return;
      }
    }
    
    checkedSubQueries.add(q);
    //1 million doesn't work
    if (mySubQueries.size() < 5000) {
      mySubQueries.add(q);
    } else {
      mySubQueries.remove(0);
      mySubQueries.add(q);
    }
  }
  
  static void checkBgpQueue(OpBGP bgp) {
    if (myBgpSubQueries.isEmpty()) {
      checkedBgpSubQueries.add(bgp);
      myBgpSubQueries.add(bgp);
      return;
    }
    
    if (checkForBgpSubQuery(bgp)) {
      checkedBgpSubQueries.add(bgp);
      return;
    }
    
    for (OpBGP b : checkedBgpSubQueries) {
      if (b.equals(bgp)) return;
    }
    
    for (OpBGP b : myBgpSubQueries) {
      if (b.equals(bgp)){
        cacheBgpQuery(bgp);
        checkedBgpSubQueries.add(bgp);
        cachedBgpSubQueries.add(bgp);
        myBgpSubQueries.remove(b);
        return;
      }
    }
    
    checkedBgpSubQueries.add(bgp);
    if (mySubQueries.size() < 100000) {
      myBgpSubQueries.add(bgp);
    } else {
      myBgpSubQueries.remove(0);
      myBgpSubQueries.add(bgp);
    }
  }

  void checkQuery(ArrayList<Query> q) {
    checkedSubQueries = new ArrayList<Query>();
    cachedSubQueries = new ArrayList<Query>();
    for (Query sq : q) {
      checkQueue(sq);
    }
  }
  
  void checkBgps(ArrayList<OpBGP> bgps) {
    checkedBgpSubQueries = new ArrayList<OpBGP>();
    cachedBgpSubQueries = new ArrayList<OpBGP>();
    for (OpBGP bgp : bgps) {
      checkBgpQueue(bgp);
    }
  }
  
  public static void main(String[] args) throws Exception {
    String s1 = "SELECT * WHERE { ?x db:p ?y . ?y db:q ?z . ?z db:r db:a . }";
    String s2 = "SELECT * WHERE { ?x db:w ?y . ?y db:r ?z . ?x db:s db:a . }";
    String s3 = "SELECT * WHERE { ?x db:w ?y . ?y db:r ?z . }";
    String s4 = "SELECT * WHERE { ?v db:p ?q . ?v db:r db:a . }";
    String s5 = "SELECT * WHERE { ?v db:f ?q . ?q db:f ?r . }";
    String s6 = "SELECT * WHERE { ?x db:p ?y . ?y db:q ?z . ?z db:r db:a . }";
    
    Parser parser = new Parser();
    Query q1 = parser.parseDbPedia(s1);
    Query q2 = parser.parseDbPedia(s2);
    Query q3 = parser.parseDbPedia(s3);
    Query q4 = parser.parseDbPedia(s4);
    Query q5 = parser.parseDbPedia(s5);
    Query q6 = parser.parseDbPedia(s6);
    
    ExperimentPolicy ep = new ExperimentPolicy();
    
    ArrayList<OpBGP> bgps1 = ExtractBgps.getBgps(Algebra.compile(q1));
    bgps1 = Joins.cleanBGPs(bgps1);
    ArrayList<ArrayList<OpBGP>> list1 = new ArrayList<ArrayList<OpBGP>>();
    
    for (int k = 0; k < bgps1.size(); k++) {
        ArrayList<OpBGP> opbl = new ArrayList<OpBGP>();
        opbl.add(bgps1.get(k));
        list1.add(opbl);
    }
    
    ArrayList<ArrayList<OpBGP>> subQueries1 = ep.getSubQueries(list1);
    Collections.sort(subQueries1, ep.subQueryComparator);
    subQueries1.remove(subQueries1.size() - 1);
    subQueries1 = ep.removeDisconnectedBgps(subQueries1);
    ArrayList<OpBGP> bgpsq1 = ep.canonicaliseBgpList(subQueries1);
    
    ep.checkBgps(bgpsq1);
    
    
    ArrayList<OpBGP> bgps2 = ExtractBgps.getBgps(Algebra.compile(q2));
    bgps2 = Joins.cleanBGPs(bgps2);
    ArrayList<ArrayList<OpBGP>> list2 = new ArrayList<ArrayList<OpBGP>>();
    
    for (int k = 0; k < bgps2.size(); k++) {
        ArrayList<OpBGP> opb2 = new ArrayList<OpBGP>();
        opb2.add(bgps2.get(k));
        list2.add(opb2);
    }
    
    ArrayList<ArrayList<OpBGP>> subQueries2 = ep.getSubQueries(list2);
    Collections.sort(subQueries2, ep.subQueryComparator);
    subQueries2.remove(subQueries2.size() - 1);
    subQueries2 = ep.removeDisconnectedBgps(subQueries2);
    ArrayList<OpBGP> bgpsq2 = ep.canonicaliseBgpList(subQueries2);
    
    ep.checkBgps(bgpsq2);
    
    
    ArrayList<OpBGP> bgps3 = ExtractBgps.getBgps(Algebra.compile(q3));
    bgps3 = Joins.cleanBGPs(bgps3);
    ArrayList<ArrayList<OpBGP>> list3 = new ArrayList<ArrayList<OpBGP>>();
    
    for (int k = 0; k < bgps3.size(); k++) {
        ArrayList<OpBGP> opb3 = new ArrayList<OpBGP>();
        opb3.add(bgps3.get(k));
        list3.add(opb3);
    }
    
    ArrayList<ArrayList<OpBGP>> subQueries3 = ep.getSubQueries(list3);
    Collections.sort(subQueries3, ep.subQueryComparator);
    subQueries3.remove(subQueries3.size() - 1);
    subQueries3 = ep.removeDisconnectedBgps(subQueries3);
    ArrayList<OpBGP> bgpsq3 = ep.canonicaliseBgpList(subQueries3);
    
    ep.checkBgps(bgpsq3);
    
    
    ArrayList<OpBGP> bgps4 = ExtractBgps.getBgps(Algebra.compile(q4));
    bgps4 = Joins.cleanBGPs(bgps4);
    ArrayList<ArrayList<OpBGP>> list4 = new ArrayList<ArrayList<OpBGP>>();
    
    for (int k = 0; k < bgps4.size(); k++) {
        ArrayList<OpBGP> opb4 = new ArrayList<OpBGP>();
        opb4.add(bgps4.get(k));
        list4.add(opb4);
    }
    
    ArrayList<ArrayList<OpBGP>> subQueries4 = ep.getSubQueries(list4);
    Collections.sort(subQueries4, ep.subQueryComparator);
    subQueries4.remove(subQueries4.size() - 1);
    subQueries4 = ep.removeDisconnectedBgps(subQueries4);
    ArrayList<OpBGP> bgpsq4 = ep.canonicaliseBgpList(subQueries4);
    
    ep.checkBgps(bgpsq4);
    
    
    ArrayList<OpBGP> bgps5 = ExtractBgps.getBgps(Algebra.compile(q5));
    bgps5 = Joins.cleanBGPs(bgps5);
    ArrayList<ArrayList<OpBGP>> list5 = new ArrayList<ArrayList<OpBGP>>();
    
    for (int k = 0; k < bgps5.size(); k++) {
        ArrayList<OpBGP> opb5 = new ArrayList<OpBGP>();
        opb5.add(bgps5.get(k));
        list5.add(opb5);
    }
    
    ArrayList<ArrayList<OpBGP>> subQueries5 = ep.getSubQueries(list5);
    Collections.sort(subQueries5, ep.subQueryComparator);
    subQueries5.remove(subQueries5.size() - 1);
    subQueries5 = ep.removeDisconnectedBgps(subQueries5);
    ArrayList<OpBGP> bgpsq5 = ep.canonicaliseBgpList(subQueries5);
    
    ep.checkBgps(bgpsq5);
    
    
    ArrayList<OpBGP> bgps6 = ExtractBgps.getBgps(Algebra.compile(q6));
    bgps6 = Joins.cleanBGPs(bgps6);
    ArrayList<ArrayList<OpBGP>> list6 = new ArrayList<ArrayList<OpBGP>>();
    
    for (int k = 0; k < bgps6.size(); k++) {
        ArrayList<OpBGP> opb6 = new ArrayList<OpBGP>();
        opb6.add(bgps6.get(k));
        list6.add(opb6);
    }
    
    ArrayList<ArrayList<OpBGP>> subQueries6 = ep.getSubQueries(list6);
    Collections.sort(subQueries6, ep.subQueryComparator);
    subQueries6.remove(subQueries6.size() - 1);
    subQueries6 = ep.removeDisconnectedBgps(subQueries6);
    ArrayList<OpBGP> bgpsq6 = ep.canonicaliseBgpList(subQueries6);
    
    ep.checkBgps(bgpsq6);
    
    System.out.println("MY BGPSUBQUERIES ARE: " + myBgpSubQueries);
    myCache.printCache();
  }
}
