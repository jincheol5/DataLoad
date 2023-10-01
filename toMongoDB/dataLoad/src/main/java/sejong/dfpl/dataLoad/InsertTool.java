package sejong.dfpl.dataLoad;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.bson.Document;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;

public class InsertTool {
	
	private static final String url = "mongodb://localhost:27017";
	
	private MongoDatabase mongoDB;
	
	private HashMap<String,ArrayList<Document>> temporalPathMap; //각 vertex를 key로, temporal path를 구성하는 edge event들을 ArrayList로 저장
	
	public InsertTool(String dbName) {
		
		this.mongoDB = MongoClients.create(url).getDatabase(dbName);
		
	}
	
	/**
	 * compute tSSP temporal path 
	 * @param sourceID
	 * @param startTime
	 */
	public void computeTSSP(String sourceID,Long startTime) {
		
		this.temporalPathMap=new HashMap<>();
		
		HashMap<String,Long> gammaTable=new HashMap<>();
		
		//initialize gamma table 
		gammaTable.put(sourceID, startTime);
		
		//initialize temporal path map
		MongoCollection<Document> vertices=this.mongoDB.getCollection("Vertex");
		
		Iterator<Document> verticesIter=vertices.find().iterator();
		while(verticesIter.hasNext()) {
			ArrayList<Document> path=new ArrayList<>();
			
			String vertexID=verticesIter.next().getString("_id");
			this.temporalPathMap.put(vertexID,path);
			
		}
		
		
		//update gamma table and temporal path map
		MongoCollection<Document> EdgeEvents=this.mongoDB.getCollection("EdgeEvent");
		
		for(Document doc:EdgeEvents.find().sort(Sorts.ascending("time"))) {
			
			if(gammaTable.containsKey(doc.getString("sourceID")) && !gammaTable.containsKey(doc.getString("targetID"))) {
				
				//update gamma table 
				gammaTable.put(doc.getString("targetID"), doc.getLong("time"));
				
				//update temporal path map			
				this.temporalPathMap.get(doc.getString("targetID")).add(doc);
				this.temporalPathMap.get(doc.getString("targetID")).addAll(0,this.temporalPathMap.get(doc.getString("sourceID")));
				
			}
			
		}
		
		
	}
	
	/**
	 * insert temporal path to mongoDB
	 * @param sourceID
	 * @param startTime
	 */
	public void insertTP(String sourceID,Long startTime) {
		
		
		String collectionName="TemporalPath"+"|"+sourceID+"|"+startTime;
		
		//compute tSSP 
		this.computeTSSP(sourceID, startTime);
		
		
		//create collection

		try {
		    this.mongoDB.createCollection(collectionName);
		} catch (Exception exception) {
		    System.err.println("Collection:- "+collectionName +" already Exists");
		}
		
		
		//insert edge event 
		MongoCollection<Document> collection=this.mongoDB.getCollection(collectionName);
		
		
		for(String key:this.temporalPathMap.keySet()) {
			
			ArrayList<Document> edgeEventList=new ArrayList<>();
			
			for(Document doc:this.temporalPathMap.get(key)) {
				edgeEventList.add(doc);
				
			}
			
			
			Document doc=new Document().append("_id", sourceID+"|"+"TR"+"|"+key).append("TemporalPath", edgeEventList);
			collection.insertOne(doc);
		}
		
		
		
	}
	
}
