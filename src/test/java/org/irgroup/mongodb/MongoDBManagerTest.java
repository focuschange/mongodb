package org.irgroup.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.include;

/**
 * <pre>
 *      org.irgroup.mongodb
 *        |_ MongoDBManagerTest.java
 * </pre>
 * <p>
 * 
 * <pre>
 *
 * </pre>
 *
 * @Author : 이상호 (focuschange@gmail.com)
 * @Date : 2016. 9. 21.
 * @Version : 1.0
 */

public class MongoDBManagerTest
{
	private final String	COLLECTION_NAME	= "terms";

	private MongoDBManager	mongo;

	@Before
	public void setUp() throws Exception
	{
		mongo = new MongoDBManager("mongodb://localhost", "test");
		System.out.println(mongo.toString());
	}

	@After
	public void tearDown() throws Exception
	{
		mongo.close();
	}

	@Test
	public void read() throws Exception
	{
		mongo.read("data/terms.txt");
		mongo.printTempDocument(10);
	}

	@Test
	public void insert() throws Exception
	{
		mongo.read("data/terms.txt");
		mongo.insert(COLLECTION_NAME);
	}

	@Test
	public void getCollection() throws Exception
	{
		MongoCollection<Document> collection = mongo.getCollection(COLLECTION_NAME);

		// excludeId()는 include 뒤에 와야 적용된다. 이상하군
		MongoCursor<Document> cursor = collection.find(BsonDocument.parse("{docid:\"1336744\"}"))
				.limit(10)
				.projection(include("docid", "terms"))
				.projection(excludeId())
				.iterator();

		while (cursor.hasNext())
			System.out.println(cursor.next().toJson());
	}

	@Test
	public void command() throws Exception
	{
		// JsonWriterSettings jws = new JsonWriterSettings(JsonMode.SHELL, "\t", "\n");
		JsonWriterSettings jws = new JsonWriterSettings(true);

		Document result = mongo.command(new Document("serverStatus", 1));
		System.out.println(result.toJson(jws));

		result = mongo.command(new Document("currentOp", 1));
		System.out.println(result.toJson(jws));
	}
}