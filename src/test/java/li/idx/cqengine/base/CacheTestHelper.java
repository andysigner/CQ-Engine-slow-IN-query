package li.idx.cqengine.base;

import static com.googlecode.cqengine.query.QueryFactory.descending;
import static com.googlecode.cqengine.query.QueryFactory.orderBy;
import static com.googlecode.cqengine.query.QueryFactory.queryOptions;
import static java.util.Arrays.asList;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.query.Query;
import com.googlecode.cqengine.resultset.ResultSet;

public class CacheTestHelper {

	private static final boolean DEBUG = false;

	private static final NullPrintStream SINK = new NullPrintStream();

	/**
	 * Generates items and fills the cache with the give size.
	 * @param items 
	 * @param size Item in collection
	 * @return 
	 */
	public static IndexedCollection<Item> fillCache(IndexedCollection<Item> items, int size) {
		List<List<String>> tags = asList(null, null, asList("t/a"), asList("t/a"), asList("t/b"), asList("t/a", "t/b"), asList("t/c"));
		String type[] = {"a", "a", "a", "b"};
	
		for (int i = 0; i < size; i++) {
			items.add(new Item("id-" + i, i, type[i % type.length], tags.get(i % tags.size())));
		}
		return items;
	}
	
	public static void search(IndexedCollection<Item> items, Query<Item> query) {
		search(items, query, 1);
	}

	public static void search(IndexedCollection<Item> items, Query<Item> query, int times) {
		// Warm up cache
		search(items, query, SINK);
	
		for (int i = 0; i < times; i++)
			search(items, query, System.out);
	}

	private static void search(IndexedCollection<Item> items, Query<Item> query, PrintStream ps) {
		long start = System.nanoTime();
		ResultSet<Item> result = items.retrieve(query, queryOptions(orderBy(descending(Item.CREATED))));
	
		if (DEBUG) {
			ps.println(query);
			ps.println("Found items: " + result.size());
			ps.println("Retrival: " + result.getRetrievalCost() + " Merge: " + result.getMergeCost());
		}
	
		Iterator<Item> iter = result.iterator();
		int i = 0;
		while (iter.hasNext() && i++ < 5) {
			Item item = iter.next();
			if (DEBUG) ps.println(item);
		}
		ps.println("Search took " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + "ms");
	}

	private static class NullPrintStream extends PrintStream {
	
			public NullPrintStream() {
				super(new OutputStream() {
	
					@Override
					public void write(int b) throws IOException {
						// Discard
					}
				});
			}
		}

	public static void logCurrentMethodName() {
		System.out.println("\nRunnning " + Thread.currentThread().getStackTrace()[2].getMethodName());
	}
	
	public static void logItemCountByQuery(IndexedCollection<Item> items, Query<Item> query) throws Exception {
		System.out.println("Items retrieved by query: "+ query + " => " + items.retrieve(query).size());
	}
}