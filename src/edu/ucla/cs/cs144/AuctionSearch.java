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

	public String getXMLDataForItemId(String itemId) throws SQLException {
		
		Connection conn = null;

        // create a connection to the database to retrieve Items from MySQL
        try {
            conn = DbManager.getConnection(true);
        } catch (SQLException ex) {
            System.out.println(ex);
        }
        
        String res = "";
        
        Statement stmt = conn.createStatement();
        String iQ = "Select * from Item where ItemId = " + itemId;
        String catQ = "Select category from ItemCategories where ItemId = " + itemId;
        String locQ = "Select l.lat, l.lon, l.locText, l.country " + 
        				" from Location l, ItemLocation i where l.ItemId = " + itemId + " and i.LocId = l.LocId";
        String sellQ = "Select SellRating, UserId from User u, Item i where i.ItemId = " + itemId +
        				" and u.UserId = i.sellId";
        String bidQ = "Select b.time, b.amount, u.UserId, u.BidRating, l.locText, l.country" +
        				" from Bid b, User u, BidLocation bl, Location l" +
        				" where b.ItemId = " + itemId + " and b.UserId = u.UserId" +
        				" and bl.UserId = b.UserId and bl.LocId = l.LocId";
        ResultSet rs = stmt.executeQuery(iQ);
        
        if(rs.next()){
        	ResultSet catRs = stmt.executeQuery(catQ);
        	ResultSet locRs = stmt.executeQuery(locQ);
        	ResultSet sellRs = stmt.executeQuery(sellQ);
        	ResultSet bidRs = stmt.executeQuery(bidQ);
        }
        
        // close the database connection
        try {
            conn.close();
        } catch (SQLException ex) {
            System.out.println(ex);
        }
        
		return "";
	}
	
	
	private String catXML(ResultSet rs) throws SQLException{
		String res = "";
		while(rs.next()){
			res += "  <Category>"+rs.getString("category")+"</Category>\n";
		}
		return res;
	}
	private String bidXML(ResultSet rs) throws SQLException{
		String res = "";
		if(rs.next()){
			res += "  <Bids>";
		} else{
			return "  <Bids />";
		}
		do {
			res += "    <Bid>\n";
			res += "      <Bidder Rating=\"" + rs.getInt("BidRating") + 
					"\" UserId=\"" + rs.getString("UserId") + "\"\n>";
			res += "        <Location>" + rs.getString("locText") + "</Location>\n";
			res += "        <Country>" + rs.getString("country") + "</Country>\n";
			res += "      </Bidder>\n";
			res += "      <Time>" + rs.getTimestamp("time") + "</Time>\n";
			res += "      <Amount>" + rs.getDouble("amount") + "</Amount>\n";
			res += "    </Bid>\n";
		} while(rs.next());
		res += "  </Bids>";
		return res;
	}
	private String locXML(ResultSet rs) throws SQLException{
		String res = "";
		rs.next();
		res += "  <Location";
		if(rs.getFloat("lat") != 0){
			res += " Latitude=\"" + rs.getFloat("lat") + "\"";
		}
		if(rs.getFloat("lon") != 0){
			res += " Latitude=\"" + rs.getFloat("lat") + "\"";
		}
		+ rs.getString("locText") + "</Location>"; 
		
	}
	
	public String echo(String message) {
		return message;
	}

}
