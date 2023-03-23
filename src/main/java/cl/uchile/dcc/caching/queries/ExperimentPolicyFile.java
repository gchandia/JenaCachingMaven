package cl.uchile.dcc.caching.queries;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.Transform;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.sparql.syntax.ElementVisitorBase;
import org.apache.jena.sparql.syntax.ElementWalker;
import org.apache.jena.tdb.TDBFactory;
import cl.uchile.dcc.caching.bgps.ExtractBgps;
import cl.uchile.dcc.caching.cache.Cache;
import cl.uchile.dcc.caching.cache.CustomCacheV5;
import cl.uchile.dcc.caching.common_joins.Joins;
import cl.uchile.dcc.caching.common_joins.Parser;
import cl.uchile.dcc.caching.transform.CacheTransformCopy;
import cl.uchile.dcc.qcan.main.SingleQuery;

public class ExperimentPolicyFile {
  private static Cache myCache;
  private static ArrayList<Query> checkedSubQueries;
  //Keeps last bgps in one query that have been checked by the function
  private static ArrayList<OpBGP> checkedBgpSubQueries;
  private static ArrayList<Query> mySubQueries;
  //Keeps last bgps that have been checked
  private static ArrayList<OpBGP> myBgpSubQueries;
  private static ArrayList<Query> cachedSubQueries;
  //Keeps last bgps that were attempted to be cached in one query
  private static ArrayList<OpBGP> cachedBgpSubQueries;
  private static String myModel = "/home/gchandia/WikiDB";
  //private static String myModel = "D:\\tmp\\WikiDB";
  private static Dataset ds = TDBFactory.createDataset(myModel);
  private static Model model;
  private static int totalTps = 0;
  private static int attemptedToCache = 0;
  private static int queryNumber = 1;
  private static String qu = "";
  
  public ExperimentPolicyFile() throws Exception {
    checkedSubQueries = new ArrayList<Query>();
    checkedBgpSubQueries = new ArrayList<OpBGP>();
    mySubQueries = new ArrayList<Query>();
    myBgpSubQueries = new ArrayList<OpBGP>();
    cachedSubQueries = new ArrayList<Query>();
    cachedBgpSubQueries = new ArrayList<OpBGP>();
    myCache = new CustomCacheV5(1000, 10000000, 900, 10);
    //myCache = new CustomCacheV6(100, 1000000, 90, 10);
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
  
  ArrayList<OpBGP> getSubQueriesV2(ArrayList<OpBGP> input) {
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
  
  ArrayList<OpBGP> getSubQueriesV3(ArrayList<OpBGP> input, OpBGP first) {
	int n = input.size() - 1;
	ArrayList<OpBGP> clone = input;
	clone.remove(0);
	ArrayList<OpBGP> output = new ArrayList<OpBGP>();
	output.add(first);
	for (int x = 0; x < n; x++) {
	  int y = clone.get(x).getPattern().size();
	  for (int i = 1; i < (1 << y); i++) {
		BasicPattern bp = new BasicPattern();
		for (int z = 0; z < first.getPattern().size(); z++) {
		  bp.add(first.getPattern().get(z));
		}
		for (int j = 0; j < y; j++) {
		  if ((i & (1 << j)) > 0) bp.add(input.get(x).getPattern().get(j));
		}
		output.add(new OpBGP(bp));
	  }
	}
	return output;
  }
  
  ArrayList<OpBGP> getSubQueriesV4(ArrayList<OpBGP> input) {
    int n = input.size();
	ArrayList<OpBGP> output = new ArrayList<OpBGP>();
	for (int x = 0; x < n; x++) {
	  int y = input.get(x).getPattern().size();
	  BasicPattern bp = new BasicPattern();
	  for (int i = 0; i < y; i++) {
		bp.add(input.get(x).getPattern().get(i));
		BasicPattern bpCopy = new BasicPattern(bp);
		output.add(new OpBGP(bpCopy));
	  }
	}
	return output;
  }
  
  Comparator<OpBGP> subQueryComparator = new Comparator<OpBGP>()
  {
      public int compare(OpBGP o1, OpBGP o2)
      {
          return Integer.compare(o2.getPattern().size(), o1.getPattern().size());
      }
  };
  
  private static boolean isConnectedBgp(OpBGP input) {
    // For each bgp, check if it shares a var with any other bgp
    boolean flag = true;
    if (input.getPattern().size() == 1) return true;
    
    for (Triple checkBgp : input.getPattern()) {
      boolean subflag = false;
      for (Triple checkedBgp : input.getPattern()) {
        if (checkBgp == checkedBgp) continue;
        if (Joins.shareVar(checkBgp, checkedBgp)) {
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
  
  ArrayList<OpBGP> removeDisconnectedBgps(ArrayList<OpBGP> input) {
    ArrayList<OpBGP> output = new ArrayList<OpBGP>();
    for (OpBGP bgp : input) {
      if (isConnectedBgp(bgp)) {
        output.add(bgp);
      }
    }
    return output;
  }
  
  public static long getTimeApproach(Query q) {
	Op alg = Algebra.compile(q);
	alg = Algebra.optimize(alg);
	QueryIterator qit = Algebra.exec(alg, model);
	
	long beforeOneResult = System.nanoTime();
	qit.next();
	long afterOneResult = System.nanoTime();
	
	return (afterOneResult - beforeOneResult);
  }
  
  ArrayList<OpBGP> canonicaliseBgpList(ArrayList<OpBGP> input) throws Exception {
    ArrayList<OpBGP> output = new ArrayList<OpBGP>();
    
    for (OpBGP bgp : input) {
      Query q = new Query();
      SingleQuery sq;
      ElementGroup elg;
      
      q = QueryFactory.make();
      q.setQuerySelectType();
      q.setQueryResultStar(true);
      elg = new ElementGroup();
      Iterator<Triple> it = bgp.getPattern().iterator();
      
      while (it.hasNext()) {
    	elg.addTriplePattern(it.next());
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
    if (myCache.cache(qBgps.get(0), qResults)) {
      /*long startLine = System.nanoTime();
      int numberOfResults = 0;
      System.out.println(qResults.hasNext());
      while (qResults.hasNext()) {
        qResults.next();
        numberOfResults++;
      }
      //long stop = System.nanoTime();
      //long resultsTime = stop - startLine;
      //results.println("Average time: " + resultsTime + " number of results: " + numberOfResults);
      //results.println("HOLA");
      //results.println("First time: " + getTimeApproach(q));
      */
      if (qResults.hasNext()) myCache.cacheTimes(qBgps.get(0), getTimeApproach(q));
      else myCache.cacheTimes(qBgps.get(0), 0);
    }
    ds.end();
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
  
  static void cleanBgpSubQueries() {
	ArrayList<OpBGP> cleanMyBgpSubQueries = new ArrayList<OpBGP>();
	for (int i = 0; i < myBgpSubQueries.size(); i++) {
	  OpBGP b = myBgpSubQueries.get(i);
	  if (b != null) {
		cleanMyBgpSubQueries.add(b);
	  }
	}
	myBgpSubQueries = cleanMyBgpSubQueries;
  }
  
  static void checkBgpQueue(OpBGP bgp) {
	cleanBgpSubQueries();
    
    if (checkForBgpSubQuery(bgp)) {
      checkedBgpSubQueries.add(bgp);
      return;
    }
    
    for (OpBGP b : checkedBgpSubQueries) {
      if (b.equals(bgp)) return;
    }
    
    for (OpBGP b : myBgpSubQueries) {
      //System.out.println("b EQUALS " + b);
      if (b.equals(bgp)) {
    	attemptedToCache++;
        cacheBgpQuery(bgp);
        checkedBgpSubQueries.add(bgp);
        cachedBgpSubQueries.add(bgp);
        myBgpSubQueries.remove(b);
        return;
      }
    }
    
    checkedBgpSubQueries.add(bgp);
    //Change bgp buffer size
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
    /*final BufferedReader tsv = 
        new BufferedReader (
                new InputStreamReader(
                        new GZIPInputStream(
                                new FileInputStream(
                                        new File("D:\\wikidata_logs\\2017-07-10_2017-08-06_organic.tsv.gz")))));*/
    
    InputStream is = new FileInputStream(new File("/home/gchandia/wikidata_logs/FilteredLogs.tsv"));
	//InputStream is = new FileInputStream(new File("D:\\wikidata_logs\\FilteredLogs.tsv"));
	
	final Scanner sc = new Scanner(is);
    
    final PrintWriter w = new PrintWriter(new FileWriter("/home/gchandia/Thesis/100KQueriesBuffer10K.txt"));
	//final PrintWriter w = new PrintWriter(new FileWriter("D:\\Thesis\\NoCacheFinal.txt"));
    
    final ExperimentPolicyFile ep = new ExperimentPolicyFile();
    
    for (int i = 1; i <= 1000; i++) {
      final Runnable stuffToDo = new Thread() {
        @Override
        public void run() {
            try {
              System.out.println("READING QUERY " + queryNumber++);
              qu = sc.nextLine();
              Parser parser = new Parser();
              
              long startLine = System.nanoTime();
              Query q = parser.parseDbPedia(qu);
              
              /*
              long afterParse = System.nanoTime();
              String ap = "Time to parse: " + (afterParse - startLine);
              //System.out.println(q);
              //System.out.println(Algebra.compile(q));
              
              //Get bgps from optimized query q
              ArrayList<OpBGP> bgps = ExtractBgps.getBgps(Algebra.optimize(Algebra.compile(q)));
              
              //Separates bgps into chunks of size 1, hence containing all triple patterns
              ArrayList<OpBGP> numberOfTPs = ExtractBgps.separateBGPs(bgps);
              
              long bpfTime = System.nanoTime();
              String bpf = "Time before prefunctions: " + (bpfTime - startLine);
              
              String gsq = "";
              String brd = "";
              String ard = "";
              String cbl = "";
              String nbgps = "";
              
              // If there are 10 or less TPs in the query
              if (numberOfTPs.size() <= 10) {
            	//Only get subqueries with highest priority TP for each bgp
              	ArrayList<OpBGP> subQueries = ep.getSubQueriesV4(bgps);
            	long gsqTime = System.nanoTime();
            	gsq = "Time to run getSubQueries: " + (gsqTime - startLine);
                //Sort subqueries from biggest to smallest
                Collections.sort(subQueries, ep.subQueryComparator);
                long brdTime = System.nanoTime();
                brd = "Time before removing disconnected bgps: " + (brdTime - startLine);
                subQueries = ep.removeDisconnectedBgps(subQueries);
                long ardTime = System.nanoTime();
                ard = "Time after removing disconnected bgps: " + (ardTime - startLine);
                nbgps = "Number of bgps to canonicalise: " + subQueries.size();
                ArrayList<OpBGP> bgpsq = ep.canonicaliseBgpList(subQueries);
                long cblTime = System.nanoTime();
                cbl = "Time after canonicalising bgpList: " + (cblTime - startLine);
                System.out.println("Number of subqueries is: " + bgpsq.size());
                ep.checkBgps(bgpsq);
              }
              
              System.out.println("CACHE SIZE IS: " + myCache.cacheSize());
              System.out.println("RESULTS SIZE IS: " + myCache.resultsSize());
              System.out.println("AMOUNT OF CONSTANTS IS: " + myCache.getConstantAmount());
              
              Op inputOp = Algebra.compile(q);
              Transform cacheTransform = new CacheTransformCopy(myCache, startLine, numberOfTPs);
              Op cachedOp = Transformer.transform(cacheTransform, inputOp);
              
              String solution = ((CacheTransformCopy) cacheTransform).getSolution();
              long beforeOptimize = System.nanoTime();
              String bo = "Time before optimizing: " + (beforeOptimize - startLine);
              
              Op opjoin = cachedOp;
              long start = System.nanoTime();
              String br = "Time before reading results: " + (start - startLine);
              
              ds.begin(ReadWrite.READ);
              
              //System.out.println(opjoin);
              
              Query qFinal = OpAsQuery.asQuery(opjoin);
              
              */
              QueryExecution qFinalExec = QueryExecutionFactory.create(q, model);
              //QueryExecution qFinalExec = QueryExecutionFactory.create(qFinal, model);
              ResultSet rs = qFinalExec.execSelect();
              
              int cacheResultAmount = 0;
              
              while (rs.hasNext()) {
                rs.next();
                cacheResultAmount++;
              }
              
              long stop = System.nanoTime();
              String ar = "Time after reading all results: " + (stop - startLine);
              
              if (cacheResultAmount >= 0) {
                /*System.out.println("FOUND ONE");
                w.println("Info for query number " + (queryNumber - 1));
                w.println(q);
                w.println(ap);
                w.println(bpf);
                w.println(gsq);
                w.println(brd);
                w.println(ard);
                w.println(nbgps);
                w.println(cbl);
                w.println(solution);
                w.println(bo);
                w.println(br);
                w.println(ar);
                w.println("Number of bgps attempted to cache: " + attemptedToCache);
                w.println("Cache size is: " + myCache.cacheSize());
                w.println("Results size is: " + myCache.resultsSize());
                w.println("Query " + (queryNumber - 1) + " Results with cache: " + cacheResultAmount);
                w.println("Number of cache hits: " + myCache.getCacheHits());
                w.println("Number of retrievals: " + myCache.getRetrievalHits());
                //w.println(myCache.getLinkedMap());*/
            	w.println("Info for query number " + (queryNumber - 1));
            	w.println(qu);
            	w.println(ar);
                w.println("");
              }
            } catch (Exception e) {//w.println("Info for query number " + (queryNumber - 1)); 
                                   //e.printStackTrace(w);
                                   //w.println();}
            }
        }
    };
    
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    @SuppressWarnings("rawtypes")
    final Future future = executor.submit(stuffToDo);
    executor.shutdown();
    
    try {
        future.get(15, TimeUnit.SECONDS);
    } catch (InterruptedException ie) {}
    catch (ExecutionException ee) {}
    catch (TimeoutException te) {}
    }
    sc.close();
    w.close();
    //results.close();
  }
}
