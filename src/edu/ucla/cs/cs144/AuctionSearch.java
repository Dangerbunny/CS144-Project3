package edu.ucla.cs.cs144;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.ArrayList;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

class SearchEngine {
    private IndexSearcher searcher = null;
    private QueryParser parser = null;

    /** Creates a new instance of SearchEngine */
    public SearchEngine() throws IOException {
        searcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(new File("/var/lib/lucene/"))));
        parser = new QueryParser("content", new StandardAnalyzer());
    }

    public TopDocs performSearch(String queryString, int n)
        throws IOException, ParseException {
            Query query = parser.parse(queryString);
            return searcher.search(query, n);
    }

    public Document getDocument(int docId)
    throws IOException {
        return searcher.doc(docId);
    }
}

public class AuctionSearch implements IAuctionSearch {

	/* 
         * You will probably have to use JDBC to access MySQL data
         * Lucene IndexSearcher class to lookup Lucene index.
         * Read the corresponding tutorial to learn about how to use these.
         *
	 * You may create helper functions or classes to simplify writing these
	 * methods. Make sure that your helper functions are not public,
         * so that they are not exposed to outside of this class.
         *
         * Any new classes that you create should be part of
         * edu.ucla.cs.cs144 package and their source files should be
         * placed at src/edu/ucla/cs/cs144.
         *
         */

    SearchEngine engine;

    public AuctionSearch(){
        try {
			engine = new SearchEngine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
	
	public SearchResult[] basicSearch(String query, int numResultsToSkip, 
			int numResultsToReturn) throws IOException, ParseException {
		TopDocs docs = engine.performSearch(query, numResultsToSkip + numResultsToReturn);
        ScoreDoc[] sDocs = docs.scoreDocs;

        SearchResult[] results = new SearchResult[sDocs.length - numResultsToSkip];

        for(int i = numResultsToSkip, k = 0; i < sDocs.length; i++, k++){
            Document doc = engine.getDocument(sDocs[i].doc);
            results[k] = new SearchResult(doc.get("iid"), doc.get("name"));
        }
		return results;
	}

	public SearchResult[] spatialSearch(String query, SearchRegion region,
			int numResultsToSkip, int numResultsToReturn) throws SQLException, IOException, ParseException {

        Connection conn = null;

        // create a connection to the database to retrieve Items from MySQL
        try {
            conn = DbManager.getConnection(true);
        } catch (SQLException ex) {
            System.out.println(ex);
        }

        Statement stmt = conn.createStatement();
        String locQuery = "select ItemId from SpatialLocation where X(locPt) > " + region.getLx()
                            + " and X(locPt) < " + region.getRx() + " and Y(locPt) > "
                            + region.getLy() + " and Y(LocPt) < " + region.getRy();
        ResultSet rs = stmt.executeQuery(locQuery);

        int size = 0;
        while(rs.next())
            size++;
        rs.first();
        rs.previous();

        SearchResult[] basicResults = basicSearch(query, 0, size);
        ArrayList<SearchResult> finalResList = new ArrayList<SearchResult>();

        HashMap<String, SearchResult> resMap = new HashMap<String,SearchResult>();

        for(SearchResult res : basicResults){
            resMap.put(res.getItemId(), res);
        }

        for(int i = 0; i < numResultsToSkip; i++)
            rs.next();

        while(rs.next()){
            String iid = "" + rs.getLong("ItemId");
            if(resMap.get(iid) != null)
                finalResList.add(resMap.get(iid));
        }


        // close the database connection
        try {
            conn.close();
        } catch (SQLException ex) {
            System.out.println(ex);
        }

        SearchResult[] finalRes = new SearchResult[finalResList.size()];
        int counter = 0;
        for(SearchResult res : finalResList){
            finalRes[counter++] = res;
        }

		return finalRes;
	}

	public String getXMLDataForItemId(String itemId) {
		// TODO: Your code here!
		return "";
	}
	
	public String echo(String message) {
		return message;
	}

}
