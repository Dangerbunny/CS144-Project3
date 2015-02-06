package edu.ucla.cs.cs144;

import java.io.IOException;
import java.io.StringReader;
import java.io.File;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;


public class Indexer {
    
    private IndexWriter indexWriter = null;

    /** Creates a new instance of Indexer */
    public Indexer() {
    }
 
    public void rebuildIndexes() {

        Connection conn = null;

        // create a connection to the database to retrieve Items from MySQL
    	try {
    	    conn = DbManager.getConnection(true);
    	} catch (SQLException ex) {
    	    System.out.println(ex);
    	}

        HashMap<Long, String> itemCategories;

    	Statement stmt = con.createStatement();
        String nameDescQ = "select ItemId, name, description from Item;"
        String catQ = "select * from Item i, ItemCategory ic where i.ItemId = ic.ItemId;"

        ResultSet rs1 = stmt.executeQuery(nameDescQ);
        ResultSet rs2 = stmt.executeQuery(catQ);

        while(rs2.next()){
            long iid = rs2.getLong("ItemId");
            String cat = rs2.getString("category");
            String icValue = itemCategories.get(iid);
            if(icValue != null){
                icValue += " " + cat;
                itemCategories.put(iid, icValue)
            }
            else{
                itemCategories.put(iid, cat);                
            }
        }
        while(rs1.next()){

            long iid = rs1.getLong("ItemId");
            String name = rs1.getString("name");
            String description = rs1.getString("description");
            String categories = itemCategories.get(iid);

            IndexWriter writer = getIndexWriter(true);
            Document doc = new Document();
            doc.add(new StringField("iid", iid, Field.Store.YES));
            doc.add(new StringField("name", name), Field.Store.YES));

            String fullSearchableText = name + " " + categories + " " + description;
            doc.add(new TextField("content", fullSearchableText, Field.Store.NO));
            writer.addDocument(doc);
        }

        closeIndexWriter();
        // close the database connection
    	try {
    	    conn.close();
    	} catch (SQLException ex) {
    	    System.out.println(ex);
    	}
    }    


    public IndexWriter getIndexWriter(boolean create) throws IOException {
        if (indexWriter == null) {
            Directory indexDir = FSDirectory.open(new File("/var/lib/lucene/"));
            IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_10_2, new StandardAnalyzer());
            indexWriter = new IndexWriter(indexDir, config);
        }
        return indexWriter;
    }
    public static void main(String args[]) {
        Indexer idx = new Indexer();
        idx.rebuildIndexes();
    }   
}
