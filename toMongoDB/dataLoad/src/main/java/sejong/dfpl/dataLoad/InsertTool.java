package sejong.dfpl.dataLoad;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.bson.Document;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Filters;

public class InsertTool {
	
	private static final String url = "mongodb://localhost:27017";
	
	private MongoDatabase mongoDB;
	
	private HashMap<String,ArrayList<Document>> temporalPathMap; //각 vertex를 key로, temporal path를 구성하는 edge event들을 ArrayList로 저장
	
	public InsertTool(String dbName) {
		
		this.mongoDB = MongoClients.create(url).getDatabase(dbName);
		
	}
	
	
	public void insertTemporalGraph(String filePath) throws IOException {
		
		
		HashSet<String> nodeSet=new HashSet<>();
		HashSet<Long> timeSet=new HashSet<>();
		
		
		//create edge event collection
		try {
		    this.mongoDB.createCollection("EdgeEvent");
		} catch (Exception exception) {
		    System.err.println("EdgeEvent collection already Exists");
		}
		
		//create temporal graph information collection
		try {
		    this.mongoDB.createCollection("Information");
		} catch (Exception exception) {
		    System.err.println("Information collection already Exists");
		}
		
		
		MongoCollection<Document> edgeEventCollection=this.mongoDB.getCollection("EdgeEvent");
		MongoCollection<Document> infoCollection=this.mongoDB.getCollection("Information");
		
		
		//data load by reading file
		@SuppressWarnings("resource")
		BufferedReader br=new BufferedReader(new FileReader(filePath));

		
		Iterator<String> iter=br.lines().iterator();
		
		
		
		while(iter.hasNext()) {
			
			String[] lineElements=iter.next().split(" ");
			
			String sourceID=lineElements[0];
			String targetID=lineElements[1];
			Long time=Long.valueOf(lineElements[2]);
			
			
			
			String edgeEventID=sourceID+"|"+targetID+"|"+time;
			
			
	
			//중복 제거 
			if(edgeEventCollection.find(Filters.eq("_id", edgeEventID)).first()!=null)
				continue;
			
			nodeSet.add(sourceID);
			nodeSet.add(targetID);
			timeSet.add(time);
			
			Document doc=new Document()
			.append("_id",edgeEventID)
			.append("sourceID",sourceID)
			.append("targetID", targetID)
			.append("time", time);
			
			edgeEventCollection.insertOne(doc);
			
			
		}
		
		
		ArrayList<String> nodeList=new ArrayList<>(nodeSet);
		ArrayList<Long> timeList=new ArrayList<>(timeSet);
		Collections.sort(timeList);
		
		
		Document graphInfo=new Document()
		.append("_id", "graphInfo")
		.append("nodeList", nodeList)
		.append("timeList", timeList);
		
		infoCollection.insertOne(graphInfo);
		
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
