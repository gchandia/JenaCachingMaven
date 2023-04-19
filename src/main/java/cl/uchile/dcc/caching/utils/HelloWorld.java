package cl.uchile.dcc.caching.utils;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.tdb.TDBFactory;

import cl.uchile.dcc.caching.bgps.ExtractBgps;
import cl.uchile.dcc.caching.cache.Cache;
import cl.uchile.dcc.caching.cache.CustomCacheV5;
import cl.uchile.dcc.caching.common_joins.Parser;

public class HelloWorld {
  public static void main(String[] args) throws ClassNotFoundException, IOException {
	System.out.println("Hello World");
	String s = "SELECT  ?var1\r\n"
			+ "WHERE\r\n"
			+ "  { ?var1  <http://www.wikidata.org/prop/direct/P138>  <http://www.wikidata.org/entity/Q676555> }";
	Parser p = new Parser();
	Query q = new Query();
	try {
		q = p.parseDbPedia(s);
	} catch (UnsupportedEncodingException e) {
		e.printStackTrace();
	}
    ArrayList<OpBGP> bgps = ExtractBgps.getBgps(Algebra.optimize(Algebra.compile(q)));
    System.out.println(bgps.get(0).getPattern().get(0) instanceof Serializable);
    System.out.println(bgps.get(0));
    System.out.println(bgps.get(0).getPattern().toString());
    String myModel = "D:\\tmp\\WikiDB";
    Dataset ds = TDBFactory.createDataset(myModel);
    ds.begin(ReadWrite.READ);
    Model model = ds.getDefaultModel();
    QueryExecution qExec = QueryExecutionFactory.create(q, model);
    QueryExecution qExecTwo = QueryExecutionFactory.create(q, model);
    ResultSet qResults = qExec.execSelect();
    ResultSet qResultsTwo = qExecTwo.execSelect();
    int ra = 0;
    while(qResultsTwo.hasNext()) {
      qResultsTwo.next();
      ra++;
    }
    System.out.println(ra);
    Cache c = new CustomCacheV5(1000, 10000000, 990, 10);
    c.cache(bgps.get(0), qResults);
    Iterator<Triple> it = bgps.get(0).getPattern().iterator();
    FileOutputStream w = null;
    ObjectOutputStream o = null;
	try {
		w = new FileOutputStream(new File("D:\\Thesis\\CacheTest.tsv"));
		o = new ObjectOutputStream(w);
	} catch (IOException e) {
		e.printStackTrace();
	}
    while (it.hasNext()) {
      try {
		o.writeObject(it.next());
		o.writeChar('\n');
		o.close();
	    w.close();
	  } catch (IOException e) {
		e.printStackTrace();
	  }
    }
    FileInputStream fi = null;
    ObjectInputStream oi = null;
	try {
	  fi = new FileInputStream(new File("D:\\Thesis\\CacheTest.tsv"));
	  oi = new ObjectInputStream(fi);
	} catch (FileNotFoundException e) {
		e.printStackTrace();
	} catch (IOException e) {
		e.printStackTrace();
	}
    Cache cc = new CustomCacheV5(1000, 10000000, 990, 10);
    Triple t = null;
    try {
		t = (Triple) oi.readObject();
	} catch (ClassNotFoundException | IOException e1) {
		e1.printStackTrace();
	}
    
    int i = 2;
    
    while (t != null) {
      try {
    	i = 4;
    	Triple tt = (Triple) oi.readObject();
      } catch (ClassNotFoundException | IOException e) { 
    	try {
    	  oi.readChar(); 
    	} catch (EOFException ee) { ee.printStackTrace(); break; }
      }
    }
    
    System.out.println(i);
    BasicPattern bp = new BasicPattern();
	  bp.add(t);
	  OpBGP bb = new OpBGP(bp);
	  Query qq = new Query();
	  ElementGroup elg;
	  qq = QueryFactory.make();
	  qq.setQuerySelectType();
	  qq.setQueryResultStar(true);
	  elg = new ElementGroup();
	  elg.addTriplePattern(bb.getPattern().get(0));
	  qq.setQueryPattern(elg);
	  ArrayList<OpBGP> qBgps = ExtractBgps.getBgps(Algebra.compile(qq));
	  QueryExecution qExecThree = QueryExecutionFactory.create(qq, model);
	  ResultSet qResultsThree = qExecThree.execSelect();
	  System.out.println(qq);
	  int rar = 0;
	  while(qResultsThree.hasNext()) {
	    qResultsThree.next();
	    rar++;
	  }
	  System.out.println(rar);
	  try {
		oi.close();
		fi.close();
	} catch (IOException e) {
		e.printStackTrace();
	}
  }
}
