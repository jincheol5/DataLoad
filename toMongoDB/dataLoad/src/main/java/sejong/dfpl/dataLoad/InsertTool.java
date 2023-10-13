package sejong.dfpl.dataLoad;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

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
	
	private String dbName;
	
	private MongoDatabase mongoDB;
	
	private HashMap<String,Long> gammaTable;
	
	private HashMap<String,ArrayList<Document>> temporalPathMap; //각 vertex를 key로, temporal path를 구성하는 edge event들을 ArrayList로 저장
	
	public InsertTool(String dbName) {
		
		this.dbName=dbName;
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
		
		//create temporal graph Vertex collection
		try {
		    this.mongoDB.createCollection("Vertex");
		} catch (Exception exception) {
		    System.err.println("Vertex collection already Exists");
		}
		
		//create temporal graph Vertex collection
		try {
		    this.mongoDB.createCollection("Time");
		} catch (Exception exception) {
		    System.err.println("Time collection already Exists");
		}
		
		
		MongoCollection<Document> edgeEventCollection=this.mongoDB.getCollection("EdgeEvent");
		MongoCollection<Document> vertexCollection=this.mongoDB.getCollection("Vertex");
		MongoCollection<Document> timeCollection=this.mongoDB.getCollection("Time");
		
		
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
			
			//중복 아니면 insert sourceID 
			if(!nodeSet.contains(sourceID)) {
				Document doc=new Document()
						.append("_id",sourceID);
				vertexCollection.insertOne(doc);
			}
			
			//중복 아니면 insert targetID 
			if(!nodeSet.contains(targetID)) {
				Document doc=new Document()
						.append("_id",targetID);
				vertexCollection.insertOne(doc);
			}
				
			//중복 아니면 insert time 
			if(!timeSet.contains(time)) {
				Document doc=new Document()
						.append("_id",time);
				timeCollection.insertOne(doc);
			}
			
			
			//set으로 중복 제거 
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
		
		
		
		
		
		
		
	}
	
	
	
	/**
	 * compute tSSP temporal path 
	 * @param sourceID
	 * @param startTime
	 */
	public void computeTSSP(String sourceID,Long startTime) {
		
		this.temporalPathMap=new HashMap<>();
		
		this.gammaTable=new HashMap<>();
		
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
		
		//compute tSSP 
		this.computeTSSP(sourceID, startTime);
		
		
		//select database 
		this.mongoDB = MongoClients.create(url).getDatabase(this.dbName+"TP");
		
		//create temporal path collection
		String tpCollectionName="TP|"+sourceID+"|"+startTime;
		try {
		    this.mongoDB.createCollection(tpCollectionName);
		} catch (Exception exception) {
		    System.err.println("Collection "+tpCollectionName +" already Exists");
		}
		
		//create temporal path edge event collection
		String tpEdgeEventCollectionName="EdgeEvent|"+sourceID+"|"+startTime;
		try {
		    this.mongoDB.createCollection(tpEdgeEventCollectionName);
		} catch (Exception exception) {
		    System.err.println("Collection "+tpEdgeEventCollectionName +" already Exists");
		}
		
		//create temporal path vertex collection
		String tpVertexCollectionName="TPVertex|"+sourceID+"|"+startTime;
		try {
		    this.mongoDB.createCollection(tpVertexCollectionName);
		} catch (Exception exception) {
		    System.err.println("Collection "+tpVertexCollectionName +" already Exists");
		}
		
		//create temporal path vertex collection
		String tptimeCollectionName="TPTime|"+sourceID+"|"+startTime;
		try {
		    this.mongoDB.createCollection(tptimeCollectionName);
		} catch (Exception exception) {
		    System.err.println("Collection "+tptimeCollectionName +" already Exists");
		}
		
		
		
		
		
		//insert temporal path graph to mongoDB
		MongoCollection<Document> temporalPath=this.mongoDB.getCollection(tpCollectionName);
		MongoCollection<Document> tpEdgeEvent=this.mongoDB.getCollection(tpEdgeEventCollectionName);
		MongoCollection<Document> tpVertex=this.mongoDB.getCollection(tpVertexCollectionName);
		MongoCollection<Document> tpTime=this.mongoDB.getCollection(tptimeCollectionName);
		
		
		HashSet<String> edgeEventIDSet=new HashSet<>();
		HashSet<String> nodeSet=new HashSet<>();
		HashSet<Long> timeSet=new HashSet<>();
		
		for(String key:this.temporalPathMap.keySet()) {
			
			
			Long preTime=startTime;
			
			//insert temporal path doc
			Document pathDoc=new Document().append("_id", sourceID+"|TP|"+key).append("TemporalPath",this.temporalPathMap.get(key));
			temporalPath.insertOne(pathDoc);
			
			
			//insert edge event,vertex,time
			for(Document doc:this.temporalPathMap.get(key)) {
				
				
				String edgeEventID=doc.getString("_id");
				String sourceVertexID=doc.getString("sourceID")+"|"+preTime;
				String targetVertexID=doc.getString("targetID")+"|"+doc.getLong("time");
				Long time=doc.getLong("time");
				
				//edge event 중복 검사 
				if(!edgeEventIDSet.contains(edgeEventID)) {
					tpEdgeEvent.insertOne(doc);
				}
				
				//source vertex 중복 검사
				if(!nodeSet.contains(sourceVertexID)) {
					Document vertexDoc=new Document()
							.append("_id",sourceVertexID)
							.append("vertexID", doc.getString("sourceID"))
							.append("time", preTime);
					
					tpVertex.insertOne(vertexDoc);
				}
				
				//target vertex 중복 검사
				if(!nodeSet.contains(targetVertexID)) {
					Document vertexDoc=new Document()
							.append("_id",targetVertexID)
							.append("vertexID", doc.getString("targetID"))
							.append("time", time);
					
					tpVertex.insertOne(vertexDoc);
				}
				
				//time 중복 검사
				if(!timeSet.contains(time)) {
					Document timeDoc=new Document()
							.append("_id",time);
					
					tpTime.insertOne(timeDoc);
				}
				
				edgeEventIDSet.add(edgeEventID);
				nodeSet.add(sourceVertexID);
				nodeSet.add(targetVertexID);
				timeSet.add(time);
				
				//preTime 재조정
				preTime=time;
				
				
			}
			
			
		}
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
	}
	
}
