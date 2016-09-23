package org.irgroup.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * <pre>
 *      org.irgroup.mongodb
 *        |_ MongoDBManager.java
 * </pre>
 * <p>
 * <pre>
 *
 * </pre>
 *
 * @Author : 이상호 (focuschange@gmail.com)
 * @Date : 2016. 9. 21.
 * @Version : 1.0
 */

public class MongoDBManager {
	private static final Logger logger	= LoggerFactory.getLogger(MongoDBManager.class);

	MongoClient mongo;
	MongoDatabase database;

	private List<Document> tempDocument = new ArrayList<Document>();

	public MongoDBManager(String uri, String db) {
		logger.trace("connect");
		mongo = new MongoClient(new MongoClientURI(uri));
		database(db);
	}

	public void database(String db)
	{
		logger.trace("get database");
		database = mongo.getDatabase(db);
	}

	public MongoCollection<Document> getCollection(String collection)
	{
		logger.trace("get collection");
		return database.getCollection(collection);
	}

	public void close()
	{
		logger.trace("connection close");
		assert mongo != null : "Mongodb connector is null";
		mongo.close();
	}

	public void read(String fileName)
	{
		logger.trace("file read");

		if(tempDocument.size() > 0)
			tempDocument.clear();

		try {
			BufferedReader reader = new BufferedReader(new FileReader(fileName));
			String line;
			int fieldFlag = 0;
			Document doc = new Document();

			int i = 0;
			while((line = reader.readLine()) != null)
			{
				if(fieldFlag == 0) // docid
				{
					doc.append("docid", line);
				}
				else if(fieldFlag == 1) // terms
				{
					doc.append("terms", line);
				}
				else if(fieldFlag == 2) // parsed terms
				{
					doc.append("parsedTerms", line);
					tempDocument.add(doc);
					doc = new Document();
				}

				fieldFlag ++;
				fieldFlag %=  3;

				i ++;
			}

			logger.trace(i + " rows complete");
			logger.trace(tempDocument.size() + " documents ");

			reader.close();
		} catch (java.io.IOException e) {
			e.printStackTrace();
		}
	}

	public void insert(String collection)
	{
		logger.trace("insert ");

		MongoCollection<Document> table = getCollection(collection);

		table.insertMany(tempDocument);

		System.out.println(table.count() + " record inserted");
	}

	public Document command(Document commandJson)
	{
		logger.trace("command : " + commandJson.toJson());

		MongoDatabase admindb = mongo.getDatabase("admin");
		return admindb.runCommand(commandJson);
	}

	public void printTempDocument(int count)
	{
		int i = 0;
		for(Document doc : tempDocument)
		{
			if(i ++ == count)
				break;

			System.out.println(doc.toJson());
		}
	}

	@Override
	public String toString() {
		return "MongoDBManager{" +
				"\nmongo=" + mongo +
				", \ndatabase=" + database +
				"\n}";
	}
}
