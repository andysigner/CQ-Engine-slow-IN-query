package li.idx.cqengine.base;

import java.util.List;

import com.googlecode.cqengine.attribute.Attribute;
import com.googlecode.cqengine.attribute.MultiValueNullableAttribute;
import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.attribute.SimpleNullableAttribute;

public class Item {

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
	public static final Attribute<Item, String> TYPE = new SimpleNullableAttribute <Item, String>("TYPE") {

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