package cl.uchile.dcc.caching.common_joins;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.zip.GZIPInputStream;

import org.apache.jena.datatypes.DatatypeFormatException;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QueryParseException;
import org.apache.jena.query.Syntax;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.expr.ExprException;

import cl.uchile.dcc.blabel.label.GraphColouring.HashCollisionException;
import cl.uchile.dcc.caching.bgps.ExtractBgps;
import cl.uchile.dcc.caching.utils.QueryBuilder;
import cl.uchile.dcc.caching.utils.QueryModifier;
import cl.uchile.dcc.qcan.main.SingleQuery;


public class Reader {
	private int nQueries;
	private File folder;
	private boolean compressed;
	private int disconnected;
	
	public Reader(int number, boolean flag) {
		this.nQueries = number;
		this.compressed = flag;
	}
	
	public Reader(File dir, int number, boolean flag) {
		this.folder = dir;
		this.nQueries = number;
		this.compressed = flag;
	}
	
	public int getDisconnected() {
		return this.disconnected;
	}
	
	public String tripleString(TriplePath t) {
		return t.getSubject() + "\t" + t.getPredicate() + "\t" + t.getObject();
	}
	
	private InputStreamReader getReader(File file) throws IOException {
		if (this.compressed) return new InputStreamReader(new GZIPInputStream(new FileInputStream(file)));
		return new InputStreamReader(new FileInputStream(file));
	}
	
	private int getFileSize(File file) throws IOException {
		int total = 0;
		BufferedReader tsv = new BufferedReader(getReader(file));
		
		while (true) {
			String line = tsv.readLine();
			if (line == null) break;
			total++;
		}
		
		return total;
	}
	
	/**
	 * Receives file as an input and returns all queries parsed.
	 * @param file
	 * @param stop
	 * @return Set with all queries from file
	 * @throws Exception
	 */
	public HashSet<Query> getAllFileQueries(File file) throws IOException {
		int total = getFileSize(file);
		HashSet<Query> set = new HashSet<Query>(total);
		
		BufferedReader tsv = new BufferedReader(getReader(file));
		Query q;
		Parser parser = new Parser();
		
		for (int i = 1; i <= total; i++) {
			String line = tsv.readLine();
			
			try {
				//System.out.println(line);
				q = parser.parseDbPedia(line);	// Parse line and turn into query q
				//System.out.println(q);
				set.add(q);
			} catch (UnsupportedOperationException e) { e.printStackTrace(); }
			
			System.out.println("Reading line: " + i + " of " + total);
		}
		
		return set;
	}
	
	/**
	 * Get a set of size N of queries from a file. If file doesn't contain N queries, returns the ones found.
	 * @param file input file
	 * @param stop how many queries you want
	 * @return Set with size N of queries
	 * @throws Exception
	 */
	public HashSet<Query> getNFileQueries(File file, int stop) throws IOException {
		HashSet<Query> set = new HashSet<Query>(stop);
		
		BufferedReader tsv = new BufferedReader(getReader(file));
		Query q;
		Parser parser = new Parser();
		int read = 0;
		
		while (read < stop) {
			String line = tsv.readLine();
			if (line == null) break;
			
			try {
				System.out.println(line);
				q = parser.parseDbPedia(line);	// Parse line and turn into query q
				System.out.println(q);
				set.add(q);
				System.out.println("Obtained " + ++read + " queries of " + stop);
			} catch (UnsupportedOperationException e) { e.printStackTrace(); }
		}
		
		System.out.println(set.size());
		return set;
	}
	
	/**
	 * Only visits files inside folder, not other folders
	 */
	public HashMap<String, Integer> visitFolderQueries() throws IOException, InterruptedException, HashCollisionException {
		HashMap<String, Integer> map = new HashMap<String, Integer>(this.nQueries * 3);
		for (File entry : this.folder.listFiles()) {
			if (!entry.isDirectory()) map.putAll(visitFileJoins(entry));
		}
		return map;
	}
	
	public HashMap<String, Integer> visitFileBgps(File file) throws IOException, InterruptedException, HashCollisionException {
		System.out.println("Begin reading: " + file.getName());
		int total = getFileSize(file);
		HashMap<String, Integer> map = new HashMap<String, Integer>(this.nQueries * 3);
		
		BufferedReader tsv;
		Parser parser = new Parser();
		ArrayList<OpBGP> canon_bgps;
		Query q;
		SingleQuery sq;
		
		if (this.compressed) tsv = new BufferedReader (new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
		else tsv = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
		
		for (int i = 1; i <= total; i++) {
			String line = tsv.readLine();
			
			try {
				q = parser.parseDbPedia(line);	// Parse line and turn into query q
				sq = new SingleQuery(q.toString(), true, true, false, true);
				q = QueryFactory.create(sq.getQuery(), Syntax.syntaxARQ);
				
				canon_bgps = ExtractBgps.getBgps(Algebra.compile(q));
				canon_bgps = Joins.cleanBGPs(canon_bgps); // Eliminates cases such as ?a ?b ?c, ?d ?e.
				
				ArrayList<ArrayList<OpBGP>> list = new ArrayList<ArrayList<OpBGP>>();
				
				for (int k = 0; k < canon_bgps.size(); k++) {
					ArrayList<OpBGP> opbl = new ArrayList<OpBGP>();
					opbl.add(canon_bgps.get(k));
					list.add(opbl);
				}
				
				ArrayList<ArrayList<OpBGP>> output = QueryBuilder.reCanonicalise(list);
				
				/*ArrayList<Query> newQueries = QueryBuilder.buildQuery(list);
				ArrayList<Query> bgpQs = new ArrayList<Query>();
				
				// Re-turn queries by canonicalising a second time
				for (Query qu : newQueries) {
					sq = new SingleQuery(qu.toString(), true, true, false, true);
					q = QueryFactory.create(sq.getQuery(), Syntax.syntaxARQ);
					bgpQs.add(q);
				}
				
				canon_bgps = new ArrayList<OpBGP>();
				
				for (Query qu : bgpQs) {
					ArrayList<OpBGP> b = ExtractBgps.getBgps(Algebra.compile(qu));
					b = Joins.cleanBGPs(b);
					canon_bgps.addAll(b);
				}
				*/
				
				while (!output.isEmpty()) {
					ArrayList<OpBGP> l = output.remove(0);
					while (!l.isEmpty()) {
						OpBGP opb = l.remove(0);
						String j = opb.toString().replace("\n", "");
						if (map.get(j) == null) map.put(j, 1);
						else map.replace(j, map.get(j) + 1);
					}
				}
			} catch (UnsupportedOperationException e) { e.printStackTrace(); }
			System.out.println("Reading line: " + i + " of " + total);
		}
		return map;
	}
	
	public HashMap<Integer, HashMap<String, Integer>> visitFileSubBgpsWithSize(File file) throws FileNotFoundException, IOException {
		System.out.println("Begin reading: " + file.getName());
		int total = getFileSize(file);
		HashMap<Integer, HashMap<String, Integer>> finalMap = new HashMap<Integer, HashMap<String, Integer>>(100); // I don't think any subbgp is over size 100
		
		BufferedReader tsv;
		Parser parser = new Parser();
		ArrayList<OpBGP> input;
		ArrayList<ArrayList<OpBGP>> subBGPs;
		Query q;
		SingleQuery sq;
		
		if (this.compressed) tsv = new BufferedReader (new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
		else tsv = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
		
		for (int i = 0; i <= total; i++) {
			String line = tsv.readLine();
			
			try {
				// Parse line and turn into query q
				q = parser.parseDbPedia(line);
				
				// Get canonical version of q
				sq = new SingleQuery(q.toString(), true, true, false, true);
				q = QueryFactory.create(sq.getQuery(), Syntax.syntaxARQ);
				
				// Get the bgps from q's algebra
				input = ExtractBgps.getSplitBgps(Algebra.compile(q));
				
				// Remove any bgps of size 1, we're only looking for sizes 2 and more
				ArrayList<OpBGP> inputClone = new ArrayList<OpBGP>();
				for (OpBGP bgp : input) {
					if (bgp.getPattern().size() != 1) inputClone.add(bgp);
				}
				input = inputClone;
				
				// Get all the subsets of bgps from our input list
				ArrayList<ArrayList<OpBGP>> c = new ArrayList<ArrayList<OpBGP>>();
				for (OpBGP bgp : input) {
					ArrayList<OpBGP> l = new ArrayList<>();
					l.add(bgp);
					c.add(l);
				}
				subBGPs = Joins.getSplitSubBGPs(c);
				
				// Transform any subject or object into a variable, as per Aidan's suggestion
				QueryModifier.obPrTransformer(subBGPs);
				
				// We turn every single ArrayList<OpBGP> into a query q
		        //ArrayList<Query> newQueries = QueryBuilder.buildQuery(subBGPs);
		        //ArrayList<ArrayList<OpBGP>> output = new ArrayList<ArrayList<OpBGP>>();
		        ArrayList<ArrayList<OpBGP>> output = QueryBuilder.reCanonicalise(subBGPs);
		        
		        // Any disconnected queries will be written down here
		        if(output.isEmpty()) this.disconnected++;
				
				// Write bgps in a map depending on size
				while (!output.isEmpty()) {
					ArrayList<OpBGP> opbs = output.remove(0);
					int size = opbs.get(0).getPattern().size();
					String j = opbs.toString().replace("\n", "");
					if (finalMap.get(size) == null) {
						finalMap.put(size, new HashMap<String, Integer>(this.nQueries));
						finalMap.get(size).put(j, 1);
					} else {
						if (finalMap.get(size).get(j) == null) finalMap.get(size).put(j, 1);
						else finalMap.get(size).replace(j, finalMap.get(size).get(j) + 1);
					}
				}
			} catch (InterruptedException | HashCollisionException e) { e.printStackTrace(); }
			System.out.println("Reading line: " + i + " of " + total);
		}
		return finalMap;
	}
	
	/*public HashMap<String, Integer> visitFileSubBgps(File file) throws Exception {
		System.out.println("Begin reading: " + file.getName());
		HashMap<String, Integer> map = new HashMap<String, Integer>(this.nQueries * 3);
		
		int start = 0;
		int stop = 1000;
		
		BufferedReader tsv;
		Parser parser = new Parser();
		ArrayList<ArrayList<OpBGP>> input;
		ArrayList<ArrayList<OpBGP>> subBGPs = new ArrayList<ArrayList<OpBGP>>();
		Query q;
		SingleQuery sq;
		
		if (this.compressed) tsv = new BufferedReader (new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
		else tsv = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
		
		while (true) {
			if (start == stop) break;
			String line = tsv.readLine();
			if (line == null) break;
			
			try {
				q = parser.parseDbPedia(line);	// Parse line and turn into query q
				sq = new SingleQuery(q.toString(), true, true, false, true);
				q = QueryFactory.create(sq.getQuery(), Syntax.syntaxARQ);
				// Debo modificar sujetos y objetos aca
				//QueryModifier.obPrTransformer(q);
				
				input = ExtractBgps.getSplitBgps(Algebra.compile(q));
				subBGPs = Joins.getSplitSubBGPs(input);
				
				for (ArrayList<OpBGP> l : subBGPs) {
					System.out.println(l.toString());
				}
				
				QueryModifier.obPrTransformer(subBGPs);
				System.out.println("**************************");
				
				for (ArrayList<OpBGP> l : subBGPs) {
					System.out.println(l.toString());
				}
				
				if(subBGPs.isEmpty()) this.disconnected++;
				
				while (!subBGPs.isEmpty()) {
					ArrayList<OpBGP> opbs = subBGPs.remove(0);
					String j = opbs.toString().replace("\n", "");
					if (map.get(j) == null) map.put(j, 1);
					else map.replace(j, map.get(j) + 1);
				}
			} catch (QueryParseException e) {}	// Catch all weird exceptions found, few cases except QPE
			catch (NullPointerException e) {}
			catch (UnsupportedOperationException e) {}
			catch (DatatypeFormatException e) {}
			catch (ExprException e) {}
			catch (StringIndexOutOfBoundsException e) {}
			catch (NoSuchElementException e) {}
			System.out.println("File queries missing: " + ++start + "/" + stop);
		}
		return map;
	}
	
	/**
	 * Input a file and receive a set of canonical triple patterns plus how many times it appears in that dataset
	 * @param file
	 * @return
	 * @throws Exception
	 */
	public HashMap<String, Integer> getCanonTriples(File file) throws FileNotFoundException, IOException, 
																	  InterruptedException, HashCollisionException {
		
		BufferedReader tsv;
		if (this.compressed) tsv = new BufferedReader (new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
		else tsv = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
		
		HashMap<String, Integer> map = new HashMap<String, Integer>(this.nQueries * 3);
		int total = getFileSize(file);
		
		Query q;
		SingleQuery sq;
		Parser parser = new Parser();
		Joins joins = new Joins();
		
		for (int i = 1; i <= total; i++) {
			String line = tsv.readLine();
			
			try {
				q = parser.parseDbPedia(line);	// Parse line and turn into query q
				
				// This is for canonicalised queries
				sq = new SingleQuery(q.toString(), true, true, false, true);
				q = QueryFactory.create(sq.getQuery(), Syntax.syntaxARQ);
				
				ArrayList<TriplePath> triples = joins.getTriples(q);	// Get triples from q
				Iterator<TriplePath> it = triples.iterator();
				
				while (it.hasNext()) {
					String j = it.next().toString();
					if (map.get(j) == null) map.put(j, 1);
					else map.replace(j, map.get(j) + 1);
				}
			} catch (InterruptedException e) {}
			// System.out.println("File queries missing: " + ++start + "/" + stop);
			System.out.println("Reading line: " + i + " of " + total);
		}
		tsv.close();
		return map;
	}
	
	/**
	 * Gets a file as input. If compressed, use flag = true, use number to define how many quries
	 * @param file
	 * @param flag
	 * @param number
	 * @throws IOException 
	 * @throws HashCollisionException 
	 * @throws InterruptedException 
	 */
	public HashMap<String, Integer> visitFileJoins(File file) throws IOException, InterruptedException, HashCollisionException {
		System.out.println("Begin reading: " + file.getName());
		
		int start = 0;
		int stop = 1000;
		
		BufferedReader tsv;
		Parser parser = new Parser();
		Joins joins = new Joins();
		String[] tripleStrings;
		HashMap<String, Integer> map = new HashMap<String, Integer>(this.nQueries * 3);
		Query q;
		SingleQuery sq;
		
		if (this.compressed) tsv = new BufferedReader (new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
		else tsv = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
		
		while (true) {
			if (nQueries == 0) break;
			if (start == stop) break;
			String line = tsv.readLine();
			if (line == null) break;
			
			try {
				q = parser.parseDbPedia(line);	// Parse line and turn into query q
				
				// This is for canonicalised queries
				sq = new SingleQuery(q.toString(), true, true, false, true);
				q = QueryFactory.create(sq.getQuery(), Syntax.syntaxARQ);
				
				ArrayList<TriplePath> triples = joins.getTriples(q);	// Get triples from q
				tripleStrings = new String[triples.size()];	// Turn triples into s,p,o form
				
				for (int i = 0; i < triples.size(); i++) tripleStrings[i] = tripleString(triples.get(i));
				Iterator<String> stringJoins = joins.getJoins(tripleStrings);	// Get iterator with joins in string form
				
				while (stringJoins.hasNext()) {
					String j = stringJoins.next();
					if (map.get(j) == null) map.put(j, 1);
					else map.replace(j, map.get(j) + 1);
				}
			} catch (UnsupportedEncodingException e) {}
			System.out.println("File queries missing: " + start + "/" + stop);
		}
		
		tsv.close();
		System.out.println("Finished reading: " + file.getName());
		return map;
	}
}
