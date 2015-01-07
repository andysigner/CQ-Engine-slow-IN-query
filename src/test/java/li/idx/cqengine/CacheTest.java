package li.idx.cqengine;

import static com.googlecode.cqengine.query.QueryFactory.and;
import static com.googlecode.cqengine.query.QueryFactory.descending;
import static com.googlecode.cqengine.query.QueryFactory.equal;
import static com.googlecode.cqengine.query.QueryFactory.in;
import static com.googlecode.cqengine.query.QueryFactory.lessThan;
import static com.googlecode.cqengine.query.QueryFactory.or;
import static com.googlecode.cqengine.query.QueryFactory.orderBy;
import static com.googlecode.cqengine.query.QueryFactory.queryOptions;
import static java.util.Arrays.asList;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.googlecode.cqengine.CQEngine;
import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.attribute.Attribute;
import com.googlecode.cqengine.attribute.MultiValueNullableAttribute;
import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.attribute.SimpleNullableAttribute;
import com.googlecode.cqengine.index.hash.HashIndex;
import com.googlecode.cqengine.index.navigable.NavigableIndex;
import com.googlecode.cqengine.index.unique.UniqueIndex;
import com.googlecode.cqengine.query.Query;
import com.googlecode.cqengine.resultset.ResultSet;

/**
 * Tests demonstrate poor search performance with IN queries.
 * @author asigner
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CacheTest {

	private static final IndexedCollection<Item> items = CQEngine.newInstance();

	private static final boolean DEBUG = false;

	private static final int TOTAL_ITEMS = 150000;

	private static final int CREATED = 60000;
	private static final String TYPE = "a";

	private static final NullPrintStream SINK = new NullPrintStream();

	@BeforeClass
	public static void init() {
		items.addIndex(UniqueIndex.onAttribute(Item.ID));
		items.addIndex(HashIndex.onAttribute(Item.TYPE));
		items.addIndex(HashIndex.onAttribute(Item.TAGS));
		items.addIndex(NavigableIndex.onAttribute(Item.CREATED));

		fillCache(TOTAL_ITEMS);
	}

	@Test
	public void test1QueryEquals() throws Exception {
		System.out.println("1 Test EQUALS query");

		Query<Item> query = createQueryEquals(TYPE, CREATED, "t/a");
		System.out.println("Query:  " + query);

		search(5, query, System.out);
		System.out.println("");
	}

	@Test
	public void test2QueryIn() throws Exception {
		System.out.println("2 Test IN query");

		Query<Item> query = createQueryIn(TYPE, CREATED, Arrays.asList("t/a", "x"));
		System.out.println("Query:  " + query);

		search(5, query, System.out);
		System.out.println("");
	}

	@Test
	public void test3QueryOr() throws Exception {
		System.out.println("3 Test manually crafted OR query");

		Query<Item> query = createQueryOr(TYPE, CREATED, Arrays.asList("t/a", "x"));
		System.out.println("Query:  " + query);

		search(5, query, System.out);
		System.out.println("");
	}

	@Test
	public void test4JumpOr() throws Exception {
		System.out.println("4 Test jump in OR query execution time (there is no jump)");

		final int CREATED_START = 64280;
		final int STEPS = 10;
		final int STEP_SIZE = 1;

		for (int i = 0; i < STEPS; i++) {
			Query<Item> query = createQueryOr(TYPE, CREATED_START + STEP_SIZE * i, Arrays.asList("t/a", "x"));
			System.out.println("Query: " + query);
			search(1, query, System.out);
		}
		System.out.println("");
	}

	@Test
	public void test5JumpIn() throws Exception {
		System.out.println("5 Test jump in IN query execution time (very long wait on when CREATED < 64287)");

		final int CREATED_START = 64280;
		final int STEPS = 10;
		final int STEP_SIZE = 1;

		for (int i = 0; i < STEPS; i++) {
			Query<Item> query = createQueryIn(TYPE, CREATED_START + STEP_SIZE * i, Arrays.asList("t/a", "x"));
			System.out.println("Query: " + query);
			search(1, query, System.out);
		}
		System.out.println("");
	}

	// Query searches a tag with equal
	private Query<Item> createQueryEquals(String type, long created, String tag) {
		return and(equal(Item.TYPE, type), lessThan(Item.CREATED, created), equal(Item.TAGS, tag));
	}

	// Query search tags with in which has a poor performance
	private Query<Item> createQueryIn(String type, long created, List<String> tags) {
		return and(equal(Item.TYPE, type), lessThan(Item.CREATED, created), in(Item.TAGS, tags));
	}

	// Rewritten IN query where the or is pulled out
	private Query<Item> createQueryOr(String type, long created, List<String> tags) {
		List<Query<Item>> subQueries = new ArrayList<Query<Item>>(tags.size());
		for (String tag : tags) {
			subQueries.add(and(equal(Item.TYPE, type), lessThan(Item.CREATED, created), equal(Item.TAGS, tag)));
		}
		return or(subQueries);
	}

	private void search(int times, Query<Item> query, PrintStream ps) {
		// Warm up cache
		search(query, SINK);

		for (int i = 0; i < times; i++)
			search(query, ps);
	}

	private void search(Query<Item> query, PrintStream ps) {
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

	/**
	 * Generates items and fills the cache with the give size.
	 * @param size Item in collection
	 */
	public static void fillCache(int size) {
		@SuppressWarnings("unchecked")
		List<List<String>> tags = asList(null, null, asList("t/a"), asList("t/a"), asList("t/b"), asList("t/a", "t/b"), asList("t/c"));
		String type[] = {"a", "a", "a", "b"};

		for (int i = 0; i < size; i++) {
			items.add(new Item("id-" + i, i, type[i % type.length], tags.get(i % tags.size())));
		}
	}

	private static class Item {

		private final String id;
		private long created;
		private String type;
		private List<String> tags;

		public Item(String id, long created, String type, List<String> tags) {
			super();
			this.id = id;
			this.created = created;
			this.type = type;
			this.tags = tags;
		}

		@Override
		public String toString() {
			return "Item [id=" + id + ", created=" + created + ", type=" + type + ", tags=" + tags + "]";
		}

		/**
		 * CQEngine attribute for accessing field {@code Item.id}.
		 */
		public static final Attribute<Item, String> ID = new SimpleAttribute<Item, String>("ID") {

			public String getValue(Item item) {
				return item.id;
			}
		};

		/**
		 * CQEngine attribute for accessing field {@code Item.created}.
		 */
		public static final Attribute<Item, Long> CREATED = new SimpleAttribute<Item, Long>("CREATED") {

			public Long getValue(Item item) {
				return item.created;
			}
		};

		/**
		 * CQEngine attribute for accessing field {@code Item.type}.
		 */
		// Note: For best performance:
		// - if this field cannot be null, replace this SimpleNullableAttribute with a SimpleAttribute
		public static final Attribute<Item, String> TYPE = new SimpleNullableAttribute<Item, String>("TYPE") {

			public String getValue(Item item) {
				return item.type;
			}
		};

		/**
		 * CQEngine attribute for accessing field {@code Item.tags}.
		 */
		// Note: For best performance:
		// - if the list cannot contain null elements change true to false in the following constructor, or
		// - if the list cannot contain null elements AND the field itself cannot be null, replace this
		// MultiValueNullableAttribute with a MultiValueAttribute (and change getNullableValues() to getValues())
		public static final Attribute<Item, String> TAGS = new MultiValueNullableAttribute<Item, String>("TAGS", true) {

			public List<String> getNullableValues(Item item) {
				return item.tags;
			}
		};

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
}
