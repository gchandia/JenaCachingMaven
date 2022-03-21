package cl.uchile.dcc.caching.queries;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.zip.GZIPInputStream;
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
import org.apache.jena.sparql.JenaTransactionException;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.Transform;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.sparql.syntax.ElementVisitorBase;
import org.apache.jena.sparql.syntax.ElementWalker;
import org.apache.jena.tdb.TDBFactory;
import cl.uchile.dcc.caching.bgps.ExtractBgps;
import cl.uchile.dcc.caching.cache.Cache;
import cl.uchile.dcc.caching.cache.LRUCache;
import cl.uchile.dcc.caching.common_joins.Joins;
import cl.uchile.dcc.caching.common_joins.Parser;
import cl.uchile.dcc.caching.transform.CacheTransformCopy;
import cl.uchile.dcc.qcan.main.SingleQuery;

public class ExperimentPolicyFileLoop {
  private static Cache myCache;
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
  private static String qu = "";

  public ExperimentPolicyFileLoop() {
    checkedSubQueries = new ArrayList<Query>();
    checkedBgpSubQueries = new ArrayList<OpBGP>();
    mySubQueries = new ArrayList<Query>();
    myBgpSubQueries = new ArrayList<OpBGP>();
    cachedSubQueries = new ArrayList<Query>();
    cachedBgpSubQueries = new ArrayList<OpBGP>();
    myCache = new LRUCache(100, 1000000);
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
    ds.begin(ReadWrite.READ);
    ArrayList<OpBGP> bgps = new ArrayList<OpBGP>();
    for (Triple t : bgp.getPattern()) {
      BasicPattern bp = new BasicPattern();
      bp.add(t);
      OpBGP b = new OpBGP(bp);
      bgps.add(b);
    }
    
    //i1 minimum number of TPs to cache the query
    if (bgps.size() < 1) {
      return;
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
    
    ArrayList<OpBGP> qBgps = ExtractBgps.getBgps(Algebra.compile(q));
    QueryExecution qExec = QueryExecutionFactory.create(q, model);
    ResultSet qResults = qExec.execSelect();
    if (myCache.cache(qBgps.get(0), qResults)) myCache.cacheConstants(q);
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
    if (myBgpSubQueries.size() < 10000) {
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
    final BufferedReader tsv = 
        new BufferedReader (
                new InputStreamReader(
                        new GZIPInputStream(
                                new FileInputStream(
                                        new File("C:\\Thesis\\2017-07-10_2017-08-06_organic.tsv.gz")))));
    
    final PrintWriter w = new PrintWriter(new FileWriter("C:\\Thesis\\tmp\\DeleteCache10.txt"));
    
    final ExperimentPolicyFile ep = new ExperimentPolicyFile();
    
    //1500 queries
    // i1 = 1, i2 = 100     DONE
    // i1 = 1, i2 = 1000    DONE
    // i1 = 1, i2 = 10000   DONE
    // i1 = 1, i2 = 100000  DONE
    // i1 = 2, i2 = 100     DONE
    // i1 = 2, i2 = 1000    DONE
    // i1 = 2, i2 = 10000   DONE
    // i1 = 2, i2 = 100000  DONE
    // i1 = 3, i2 = 100     DONE
    // i1 = 3, i2 = 1000    DONE
    // i1 = 3, i2 = 10000   DONE
    // i1 = 3, i2 = 100000  DONE
    // i1 = 4, i2 = 100     DONE
    // i1 = 4, i2 = 1000    DONE
    // i1 = 4, i2 = 10000   DONE
    // i1 = 4, i2 = 100000  DONE
    
    //2500 queries
    // i1 = 1, i2 = 100     DONE
    // i1 = 1, i2 = 1000    DONE
    // i1 = 1, i2 = 10000   DONE
    // i1 = 1, i2 = 100000  DONE
    // i1 = 2, i2 = 100     DONE
    // i1 = 2, i2 = 1000    DONE
    // i1 = 2, i2 = 10000   DONE
    // i1 = 2, i2 = 100000  DONE
    // i1 = 3, i2 = 100     DONE
    // i1 = 3, i2 = 1000    DONE
    // i1 = 3, i2 = 10000   DONE
    // i1 = 3, i2 = 100000  DONE
    // i1 = 4, i2 = 100     DONE
    // i1 = 4, i2 = 1000    DONE
    // i1 = 4, i2 = 10000   DONE
    // i1 = 4, i2 = 100000  DONE
    
    for (int i = 1; i <= 10; i++) {
      final Runnable stuffToDo = new Thread() {
        @Override
        public void run() {
          try {
            qu = tsv.readLine();
          } catch (IOException e1) {
            e1.printStackTrace();
          }
          
          while (true) {
            try {
              System.out.println("Reading query " + queryNumber);
              Parser parser = new Parser();
              
              long startLine = System.nanoTime();
              Query q = parser.parseDbPedia(qu);
              long afterParse = System.nanoTime();
              String ap = "Time to parse: " + (afterParse - startLine);
              //System.out.println(q);
              
              ArrayList<OpBGP> bgps = ExtractBgps.getBgps(Algebra.compile(q));
              bgps = Joins.cleanBGPs(bgps);
              ArrayList<ArrayList<OpBGP>> list = new ArrayList<ArrayList<OpBGP>>();
               
              for (int k = 0; k < bgps.size(); k++) {
                ArrayList<OpBGP> opb = new ArrayList<OpBGP>();
                opb.add(bgps.get(k));
                list.add(opb);
              }
              
              // If there are 10 or less TPs in the query
              if (list.size() <= 10) {
                ArrayList<ArrayList<OpBGP>> subQueries = ep.getSubQueries(list);
                Collections.sort(subQueries, ep.subQueryComparator);
                subQueries.remove(subQueries.size() - 1);
                subQueries = ep.removeDisconnectedBgps(subQueries);
                ArrayList<OpBGP> bgpsq = ep.canonicaliseBgpList(subQueries);
                System.out.println("Number of subqueries is: " + bgpsq.size());
                ep.checkBgps(bgpsq);
              }
              
              Op inputOp = Algebra.compile(q);
              Transform cacheTransform = new CacheTransformCopy(myCache, startLine);
              Op cachedOp = Transformer.transform(cacheTransform, inputOp);
              
              String solution = ((CacheTransformCopy) cacheTransform).getSolution();
              long beforeOptimize = System.nanoTime();
              String bo = "Time before optimizing: " + (beforeOptimize - startLine);
              
              Op opjoin = Algebra.optimize(cachedOp);
              long start = System.nanoTime();
              String br = "Time before reading results: " + (start - startLine);
              
              ds.begin(ReadWrite.READ);
              Query qFinal = OpAsQuery.asQuery(opjoin);
              QueryExecution qFinalExec = QueryExecutionFactory.create(qFinal, model);
              ResultSet rs = qFinalExec.execSelect();
              
              int cacheResultAmount = 0;
              
              while (rs.hasNext()) {
                rs.next();
                cacheResultAmount++;
              }
              
              long stop = System.nanoTime();
              String ar = "Time after reading all results: " + (stop - startLine);
              
              if (cacheResultAmount >= 0) {
                System.out.println("FOUND ONE");
                w.println("Info for query number " + (queryNumber));
                w.println(q);
                w.println(ap);
                w.println(solution);
                w.println(bo);
                w.println(br);
                w.println(ar);
                w.println("Cache size is: " + myCache.cacheSize());
                w.println("Query " + (queryNumber - 1) + " Results with cache: " + cacheResultAmount);
                w.println("");
              }
              queryNumber++;
            } catch (JenaTransactionException e1) {}
              catch (Exception e) {
                queryNumber++;
                break;
            }
          }
        }
      };
      
      final ExecutorService executor = Executors.newSingleThreadExecutor();
      @SuppressWarnings("rawtypes")
      final Future future = executor.submit(stuffToDo);
      executor.shutdown();
    
      try {
        future.get(1, TimeUnit.MINUTES);
      } catch (InterruptedException ie) {}
      catch (ExecutionException ee) {}
      catch (TimeoutException te) {}
    }
    w.close();
  }
}
