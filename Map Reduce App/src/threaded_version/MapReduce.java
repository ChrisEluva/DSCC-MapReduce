package threaded_version;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MapReduce {

	public static void main(String[] args) {
		System.out.println("Original Threaded version\n");
		// INPUT:
		
		final long startInputFiles = System.currentTimeMillis();
		Map<String, String> input = new HashMap<String, String>();

		List<Path> paths = new ArrayList<>();
		for(int i=0;i<args.length;i++){
			paths.add(Paths.get("Text Files/"+args[i]+".txt"));
		}
		
		for (Path path : paths) {
			List<String> fileLines = null;
			try {
				fileLines = Files.readAllLines(path);
			} catch (IOException e2) {
			}

			String content = "";
			for (String line : fileLines) {
				content += " " + line;
			}
			input.put(path.getFileName().toString(), content);
		}

		final long endInputFiles = System.currentTimeMillis();
		final long totalInputTime = endInputFiles - startInputFiles;
		System.out.println("Time taken to input files: " + totalInputTime+"\n");

		// APPROACH #3: Distributed MapReduce
		{
			final long startTime = System.currentTimeMillis();
			final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

			// MAP:
			
			final long mapStartTime = System.currentTimeMillis();
			final List<MappedItem> mappedItems = new LinkedList<MappedItem>();

			final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {

				@Override
				public synchronized void mapDone(String file, List<MappedItem> results) {
					mappedItems.addAll(results);
				}
			};

			List<Thread> mapCluster = new ArrayList<Thread>(input.size());

			Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
			int mappingRunnables = 0;
			while (inputIter.hasNext()) {
				Map.Entry<String, String> entry = inputIter.next();
				final String file = entry.getKey();
				final String contents = entry.getValue();
				mappingRunnables++;
				Thread t = new Thread(new Runnable() {

					@Override
					public void run() {
						map(file, contents, mapCallback);
					}
				});
				mapCluster.add(t);
				t.start();
			}
			System.out.println("No. of mapping runnable tasks: "+mappingRunnables);
			System.out.println("No. of mapping threads: "+mapCluster.size());
			// wait for mapping phase to be over:
			for (Thread t : mapCluster) {
				try {
					t.join();
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
			final long mapEndTime = System.currentTimeMillis();
			final long totalMapTime = mapEndTime - mapStartTime;
			System.out.println("Total Mapping Time: "+totalMapTime+"\n");
			// GROUP:

			Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

			Iterator<MappedItem> mappedIter = mappedItems.iterator();
			while (mappedIter.hasNext()) {
				MappedItem item = mappedIter.next();
				String word = item.getWord();
				String file = item.getFile();
				List<String> list = groupedItems.get(word);
				if (list == null) {
					list = new LinkedList<String>();
					groupedItems.put(word, list);
				}
				list.add(file);
			}

			// REDUCE:
			final long reduceStartTime = System.currentTimeMillis();
			final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {

				@Override
				public synchronized void reduceDone(String k, Map<String, Integer> v) {
					output.put(k, v);
				}
			};

			List<Thread> reduceCluster = new ArrayList<Thread>(groupedItems.size());

			Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
			int reduceRunnables = 0;
			while (groupedIter.hasNext()) {
				Map.Entry<String, List<String>> entry = groupedIter.next();
				final String word = entry.getKey();
				final List<String> list = entry.getValue();
				reduceRunnables++;
				Thread t = new Thread(new Runnable() {

					@Override
					public void run() {
						reduce(word, list, reduceCallback);
					}
				});
				reduceCluster.add(t);
				t.start();
			}
			System.out.println("No. of reduce runnable tasks: "+ reduceRunnables);
			System.out.println("No. of reduce threads: "+ reduceCluster.size());
			// wait for reducing phase to be over:
			for (Thread t : reduceCluster) {
				try {
					t.join();
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
			final long reduceEndTime = System.currentTimeMillis();
			final long totalReduceTime = reduceEndTime - reduceStartTime;
			System.out.println("Total Reduce Time: "+totalReduceTime+"\n");
			final long endTime = System.currentTimeMillis();
			final long totalExecutionTime = endTime - startTime;
			System.out.println("Total Execution Time: " + totalExecutionTime);
			
			try {
				PrintWriter w = new PrintWriter("Output/threaded_version_output.txt", "UTF-8");
				w.write("Total Mapping Time: "+totalMapTime+"\nTotal Reducing Time: "+totalReduceTime+"\nTotal Execution Time: "+totalExecutionTime+"\nOutput:"+output);
				w.close();
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
			} catch (UnsupportedEncodingException e1) {
				e1.printStackTrace();
			}
			
		}

	}

	public static interface MapCallback<E, V> {

		public void mapDone(E key, List<V> values);
	}

	public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
		String[] words = contents.trim().split("\\s+");
		List<MappedItem> results = new ArrayList<MappedItem>(words.length);
		for (String word : words) {
			results.add(new MappedItem(word, file));
		}
		callback.mapDone(file, results);
	}

	public static interface ReduceCallback<E, K, V> {

		public void reduceDone(E e, Map<K, V> results);
	}

	public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {

		Map<String, Integer> reducedList = new HashMap<String, Integer>();
		for (String file : list) {
			Integer occurrences = reducedList.get(file);
			if (occurrences == null) {
				reducedList.put(file, 1);
			} else {
				reducedList.put(file, occurrences.intValue() + 1);
			}
		}
		callback.reduceDone(word, reducedList);
	}

	private static class MappedItem {

		private final String word;
		private final String file;

		public MappedItem(String word, String file) {
			this.word = word;
			this.file = file;
		}

		public String getWord() {
			return word;
		}

		public String getFile() {
			return file;
		}

		@Override
		public String toString() {
			return "[\"" + word + "\",\"" + file + "\"]";
		}
	}
}
