package com.infiniteai.etl.handler;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.cloudant.client.api.ClientBuilder;
import com.cloudant.client.api.CloudantClient;
import com.cloudant.client.api.Database;
import com.cloudant.client.api.model.Response;
import com.cloudant.client.api.views.Key.Type;
import com.cloudant.client.org.lightcouch.DocumentConflictException;
import com.cloudant.client.org.lightcouch.NoDocumentException;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class CouchDbHandler {
	public static final String DB_URL = "url";
	public static final String DB_PORT = "port";
	public static final String DB_USERNAME = "username";
	public static final String DB_PASSWORD = "password";
	private String dbName = "";

	public CouchDbHandler(String dbName) {
		this.dbName = dbName;
	}

	private static CloudantClient getDBClient() {
		CloudantClient client = null;
		try {
			String couchDBURL = System.getenv("couchDbUrlETL");
			String couchDBPort = System.getenv("couchDbPort");
			String couchDBUser = System.getenv("couchDBUser");
			String couchDBPass = System.getenv("couchDBPass");

			client = ClientBuilder.url(new URL(couchDBURL + ":" + couchDBPort)).username(couchDBUser)
					.password(couchDBPass).build();
		} catch (MalformedURLException ex) {
			ex.printStackTrace();
		}

		return client;
	}

	public JsonObject getDocumentById(String documentId) throws Exception {

		Database db = null;
		JsonObject dbObject = null;
		CloudantClient client = getDBClient();

		if (client != null) {
			try {
				db = client.database(dbName, false);
				dbObject = db.find(JsonObject.class, documentId);
			} catch (NoDocumentException ex) {
				throw new NoDocumentException(ex.getMessage());
			} catch (Exception ex) {
				throw new Exception(ex.getMessage(), ex);
			}
		}

		return dbObject;
	}


	public List<JsonObject> getAllDocuments() throws Exception {
		Database db = null;
		List<JsonObject> documents = new ArrayList<JsonObject>();
		int retryCount = 0;

		while (retryCount < 3) {
			CloudantClient client = getDBClient();
			if (client != null) {
				try {
					db = client.database(dbName, false);
					documents = db.getAllDocsRequestBuilder().includeDocs(true).build().getResponse()
							.getDocsAs(JsonObject.class);
					break;
				} catch (NoDocumentException ex) {
					retryCount++;
					if (retryCount == 3) {
						throw new NoDocumentException(ex.getMessage());
					}
				} catch (Exception ex) {
					retryCount++;
					if (retryCount == 3) {
						throw new Exception(ex.getMessage(), ex);
					}
				}
			}
		}

		return documents;
	}

	public String saveDocument(JsonObject document) throws Exception {
		Database db = null;
		int retryCount = 0;
		String id = "";

		while (retryCount < 3) {
			CloudantClient client = getDBClient();
			if (client != null) {
				try {
					db = client.database(dbName, false);
					document.addProperty("created_at", Instant.now().getEpochSecond());
					Response response = db.save(document);
					id = response.getId();
					break;
				} catch (DocumentConflictException e) {
					id = "Document already exists with this _id";
					break;
				} catch (Exception ex) {
					retryCount++;
					if (retryCount == 3) {
						throw new Exception(ex.getMessage(), ex);
					}
				}
			}
		}

		return id;
	}

	public String saveDocumentBulk(List<JsonObject> documents) throws Exception {
		Database db = null;
		int retryCount = 0;
		String id = "";

		while (retryCount < 3) {
			CloudantClient client = getDBClient();
			if (client != null) {
				try {
					db = client.database(dbName, false);
					db.bulk(documents);
					break;
				} catch (Exception ex) {
					retryCount++;
					if (retryCount == 3) {
						throw new Exception(ex.getMessage(), ex);
					}
				}
			}
		}

		return id;
	}

	public String updateDocument(JsonObject document) throws Exception {
		Database db = null;
		int retryCount = 0;
		String id = "";

		while (retryCount < 3) {
			CloudantClient client = getDBClient();
			if (client != null) {
				try {
					db = client.database(dbName, false);
					JsonObject temp = db.find(JsonObject.class, document.get("_id").getAsString());
					document.add("created_at", temp.get("created_at"));
					document.addProperty("modified_at", Instant.now().getEpochSecond());
					Response response = db.update(document);
					id = response.getId();
					break;
				} catch (DocumentConflictException e) {
					JsonObject temp = db.find(JsonObject.class, document.get("_id").getAsString());
					document.add("_rev", temp.get("_rev"));
				} catch (Exception ex) {
					retryCount++;
					if (retryCount == 3) {
						throw new Exception(ex.getMessage(), ex);
					}
				}
			}
		}

		return id;
	}

	public String deleteDocument(String docId) throws Exception {
		Database db = null;
		int retryCount = 0;
		String id = "";

		while (retryCount < 3) {
			CloudantClient client = getDBClient();
			if (client != null) {
				try {
					db = client.database(dbName, false);
					JsonObject document = db.find(JsonObject.class, docId);
					Response response = db.remove(document);
					id = response.getId();
					break;
				} catch (Exception ex) {
					retryCount++;
					if (retryCount == 3) {
						throw new Exception(ex.getMessage(), ex);
					}
				}
			}
		}

		return id;
	}


	public class JSONComparator implements Comparator<JsonObject> {
		public int compare(JsonObject s1, JsonObject s2) {
			if (s1.has("created_at") && s2.has("created_at")) {
				return Long.compare(s1.get("created_at").getAsLong(), s2.get("created_at").getAsLong());
			} else {
				return 0;
			}
		}
	}

}
