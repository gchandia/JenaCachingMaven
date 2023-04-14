package cl.uchile.dcc.caching.common_joins;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import cl.uchile.dcc.blabel.label.GraphColouring.HashCollisionException;


public class Controller {
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws IOException, InterruptedException, HashCollisionException {
		/*
		 * IMPORTANTE: Un par de notas aqui. Primero, es imposible hacer retrieve de las queries totales;
		 * asi que insistir con triplas o bgps nomas. Piden mucha memoria, tendria que hacer uso de HDD
		 * para ir guardandolas, y no vale la pena.
		 */
		
		Reader reader = new Reader(new File("D\\wikidata_logs\\"), 10000, false);
		HashMap<String, Integer> singleMap = reader.visitFileBgps(new File ("D:\\wikidata_logs\\wikidata_1K_10.tsv"));
		HashMap<Integer, HashMap<String, Integer>> map = reader.visitFileSubBgpsWithSize(new File("D:\\wikidata_logs\\wikidata_1K_10.tsv"));
		
		@SuppressWarnings("rawtypes")
		Iterator mi = map.entrySet().iterator();
		
		Writer singleWriter = new Writer("D:\\wikidata_logs\\results\\subbgps_wikidata_1K_10_size_1.tsv");
		singleWriter.writeJoins(singleMap);
		while (mi.hasNext()) {
			Map.Entry<Integer, HashMap<String, Integer>> mapEntry = (Map.Entry<Integer, HashMap<String,Integer>>) mi.next();
			Writer writer = new Writer ("D:\\wikidata_logs\\results\\subbgps_wikidata_1K_10_size_" + mapEntry.getKey() + ".tsv");
			writer.writeJoins(mapEntry.getValue());
		}
		
		/*Reader reader = new Reader(new File("D:\\wikidata_logs\\"), 100000, false);
		HashMap<String, Integer> singleMap = reader.visitFileBgps(new File ("D:\\tmp\\bgps\\Tr2.txt"));
		HashMap<Integer, HashMap<String, Integer>> map = reader.visitFileSubBgpsWithSize(new File("D:\\tmp\\bgps\\Tr2.txt"));
		
		@SuppressWarnings("rawtypes")
		Iterator mi = map.entrySet().iterator();
		
		Writer singleWriter = new Writer("D:\\tmp\\bgps_results\\subbgps_log_17_size_1.tsv");
		singleWriter.writeJoins(singleMap);
		while (mi.hasNext()) {
			Map.Entry<Integer, HashMap<String, Integer>> mapEntry = (Map.Entry<Integer, HashMap<String,Integer>>) mi.next();
			Writer writer = new Writer ("D:\\tmp\\subbgps_log_17_size_" + mapEntry.getKey() + ".tsv");
			writer.writeJoins(mapEntry.getValue());
		}*/
		
		/*
		BufferedReader tsv = new BufferedReader (
				  new InputStreamReader(
				  new GZIPInputStream(
				  new FileInputStream(new File("D:\\Descargas\\latest-all.json.gz")))));
		
		ZipFile zipFile = new ZipFile("D:\\Descargas\\10KEntities.zip");
		Enumeration<? extends ZipEntry> entries = zipFile.entries();
		InputStream stream = null;
		
		while(entries.hasMoreElements()) {
			ZipEntry entry = entries.nextElement();
			stream = zipFile.getInputStream(entry);
		}
		
		BufferedReader tsvTwo = new BufferedReader(new InputStreamReader(stream));
		System.out.println(tsvTwo.readLine());
		String line = tsvTwo.readLine();
		int i = 1;
		while(true) {
			if (line == null) break;
			line = tsvTwo.readLine();
			System.out.println(i++);
		}
		System.out.println("TOTAL AMOUNT IS: " + i);
		
		/*
		BufferedReader tsv = new BufferedReader (
				  new InputStreamReader(
				  new GZIPInputStream(
				  new FileInputStream(new File("D:\\Descargas\\latest-all.json.gz")))));
		tsv.readLine();
		BufferedWriter w = new BufferedWriter(new FileWriter (new File("D:\\tmp\\10KDataset.json")));
		w.write("[\n");
		
		for (int i = 1; i <= 10000; i++) {
			String line = tsv.readLine();
			JSONObject obj = JsonReader.extractObject(line);
			w.write(obj.toJSONString() + ",\n");
			System.out.println("Reading line " + i + " of 10000");
		}
		
		w.write("]");
		w.close();
		*/
		//VisitFile.readLastNLines(new File("/home/gch1204/tmp2/final_joins.tsv"), 10);
		//162763
		/*Reader reader = new Reader(new File("D:\\wikidata_logs\\"), 100000, true);
		HashMap<String, Integer> singleMap = reader.visitFileBgps(new File ("D:\\wikidata_logs\\2017-06-12_2017-07-09_organic.tsv.gz"));
		HashMap<Integer, HashMap<String, Integer>> map = reader.visitFileSubBgpsWithSize(new File("D:\\wikidata_logs\\2017-06-12_2017-07-09_organic.tsv.gz"));
		
		@SuppressWarnings("rawtypes")
		Iterator mi = map.entrySet().iterator();
		
		Writer singleWriter = new Writer("D:\\tmp\\subbgps_recanon_wikidata_log_1_size_1.tsv");
		singleWriter.writeJoins(singleMap);
		while (mi.hasNext()) {
			Map.Entry<Integer, HashMap<String, Integer>> mapEntry = (Map.Entry<Integer, HashMap<String,Integer>>) mi.next();
			Writer writer = new Writer ("D:\\tmp\\subbgps_wikidata_log_1_size_" + mapEntry.getKey() + ".tsv");
			writer.writeJoins(mapEntry.getValue());
		}
		
		
		
		/*Reader reader = new Reader(new File("D:\\dbpegia_logs\\"), 100000, true);
		//HashMap<String, Integer> mapOne = reader.visitFileBgps(new File("D:\\dbpedia_logs\\access.log-20100531.gz"));
		HashMap<String, Integer> mapTwo = reader.visitFileSubBgps(new File("D:\\dbpedia_logs\\access.log-20100604.gz"));
		//Writer writerOne = new Writer("D:\\tmp\\bgps_dbpedia_log_1.tsv");
		Writer writerTwo = new Writer("D:\\tmp\\subbgps_dbpedia_log_1.tsv");
		//writerOne.writeJoins(mapOne);
		//writerTwo.writeJoins(mapTwo);
		String msg = "Number of disconnected queries are " + reader.getDisconnected();
		Files.write(Paths.get("D:\\tmp\\disc1.txt"), msg.getBytes());
		System.out.println("Number of disconnected queries are " + reader.getDisconnected());
		
		/* HashMap<String, Integer> map = reader.getCanonTriples(new File("D:\\dbpedia_logs\\access.log-20100531.gz"));
		Writer writer = new Writer("D:\\tmp\\canon_triples_dbpedia_log_1.tsv");
		writer.writeJoins(map);
		
		/*HashMap<String, Integer> map = new HashMap<String, Integer>(5000000);
		Reader reader = new Reader(new File("D:\\wikidata_logs\\"), 5000000, true);
		map = reader.visitFileQueries(new File("D:\\wikidata_logs\\2017-06-12_2017-07-09_organic.tsv.gz"));
		Writer writer = new Writer("D:\\tmp\\wd_joins_1k.tsv");
		writer.write(map);
		
		Joiner j = new Joiner();
		j.countLines(new File("/home/gch1204/tmp/"));
		HashMap<String, Integer> map = j.joinFolder(new File("/home/gch1204/tmp/"));
		HashMap<String, Integer> finalMap = MapValues.sortByValue(map);
		Writer writer = new Writer("/home/gch1204/tmp2/final_joins.tsv");
		writer.write(finalMap);
		
		Reader reader = new Reader(new File("/home/gch1204/dbpedia_logs/"), 5000000, true);
		
		map = reader.visitFileQueries(new File("/home/gch1204/dbpedia_logs/access.log-20100430.gz"));
		Writer writer = new Writer("/home/gch1204/tmp/joins_1.tsv");
		map = reader.visitFileQueries(new File("/home/gch1204/dbpedia_logs/access.log-20100502.gz"));
		Writer writer = new Writer("/home/gch1204/tmp/joins_2.tsv");
		map = reader.visitFileQueries(new File("/home/gch1204/dbpedia_logs/access.log-20100512.gz"));
		Writer writer = new Writer("/home/gch1204/tmp/joins_3.tsv");
		map = reader.visitFileQueries(new File("/home/gch1204/dbpedia_logs/access.log-20100514.gz"));
		Writer writer = new Writer("/home/gch1204/tmp/joins_4.tsv");
		map = reader.visitFileQueries(new File("/home/gch1204/dbpedia_logs/access.log-20100528.gz"));
		Writer writer = new Writer("/home/gch1204/tmp/joins_5.tsv");
		map = reader.visitFileQueries(new File("/home/gch1204/dbpedia_logs/access.log-20100531.gz"));
		Writer writer = new Writer("/home/gch1204/tmp/joins_6.tsv");
		map = reader.visitFileQueries(new File("/home/gch1204/dbpedia_logs/access.log-20100604.gz"));
		Writer writer = new Writer("/home/gch1204/tmp/joins_7.tsv");
		map = reader.visitFileQueries(new File("/home/gch1204/dbpedia_logs/access.log-20100609.gz"));
		Writer writer = new Writer("/home/gch1204/tmp/joins_8.tsv");
		map = reader.visitFileQueries(new File("/home/gch1204/dbpedia_logs/access.log-20100618.gz"));
		Writer writer = new Writer("/home/gch1204/tmp/joins_9.tsv"); // Aun por hacer
		/*map = reader.visitFileQueries(new File("/home/gch1204/dbpedia_logs/access.log-20100620.gz"));
		Writer writer = new Writer("/home/gch1204/tmp/joins_10.tsv");
		map = reader.visitFileQueries(new File("/home/gch1204/dbpedia_logs/access.log-20100702.gz"));
		Writer writer = new Writer("/home/gch1204/tmp/joins_11.tsv");
		map = reader.visitFileQueries(new File("/home/gch1204/dbpedia_logs/access.log-20100703.gz"));
		Writer writer = new Writer("/home/gch1204/tmp/joins_12.tsv");
		/*map = reader.visitFileQueries(new File("/home/gch1204/dbpedia_logs/access.log-20100713.gz"));
		Writer writer = new Writer("/home/gch1204/tmp/joins_13.tsv");
		map = reader.visitFileQueries(new File("/home/gch1204/dbpedia_logs/access.log-20100720.gz"));
		Writer writer = new Writer("/home/gch1204/tmp/joins_14.tsv");
		
		
		writer.write(map);
		int max = getMax(map.values());
		System.out.println("Different number of joins found: " + map.size());
		System.out.println("Most common join has " + max);
		System.out.println(getKey(map, max));*/
	}
}
			