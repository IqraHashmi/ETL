package com.infiniteai.etl.naseem;

import java.io.FileInputStream;
import java.util.Map.Entry;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.ValueEventListener;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.infiniteai.etl.handler.CouchDbHandler;

public class ETL {

	private static volatile int counter = 0;
	 
    public static synchronized void increment() {
        counter++;
    }
    
	public static void main(String[] args) {
		initialize();

		final FirebaseDatabase database = FirebaseDatabase.getInstance();
		saveFirebaseToCouchDb(database);
		//getStudentData(database);
		
		while(counter < 1) {
			Thread.onSpinWait();
		}
		
	}
	
	// save Single Firebase node data in couchDb
	private static void saveFirebaseToCouchDb(final FirebaseDatabase database) {
		DatabaseReference ref = database.getReference("<NODE NAME>");
		ref.addListenerForSingleValueEvent(new ValueEventListener() {
			public void onDataChange(DataSnapshot dataSnapshot) {
				CouchDbHandler couchDb=new CouchDbHandler("<DB NAME>");
				try {
					Object object = dataSnapshot.getValue(Object.class);
					String nodeStr = new Gson().toJson(object);
					JsonElement nodeJson = JsonParser.parseString(nodeStr);
					if (nodeJson.isJsonArray()) {
						JsonArray nodeArr = nodeJson.getAsJsonArray();
						for (JsonElement el : nodeArr) {
							if (el.isJsonObject()) {
								JsonObject node = el.getAsJsonObject();
								couchDb.saveDocument(node);        //save data in couchDb
							}
						}
					} else if (nodeJson.isJsonObject()) {
						for (Entry<String, JsonElement> entry : nodeJson.getAsJsonObject().entrySet()) {
							String key = entry.getKey();
							JsonElement value = entry.getValue();
							JsonObject node=value.getAsJsonObject();
							couchDb.saveDocument(node);            //save data in couchDb
						}
					}
				} catch (Exception e) {
					System.out.println(e.getMessage());
				}
				
				increment();
			}

			public void onCancelled(DatabaseError databaseError) {
				System.out.println("The read failed: " + databaseError.getCode());
				increment();
			}
		});
	}

	private static void initialize() {
		try {
			FileInputStream serviceAccount = new FileInputStream("./serviceaccount.json");  // firebase service account file Contains PRIVATE KEY etc

			FirebaseOptions options = FirebaseOptions.builder()
					.setCredentials(GoogleCredentials.fromStream(serviceAccount))
					.setDatabaseUrl("<FIREBASE REALTIME DB URL>").build();  //firebase DB url

			FirebaseApp.initializeApp(options);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
