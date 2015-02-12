package edu.ucla.cs.cs144;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
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

    SimpleDateFormat outFormatter =
            new SimpleDateFormat("MMM-dd-yy HH:mm:ss");
    SimpleDateFormat inFormatter = 
    		new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    NumberFormat fmt = NumberFormat.getCurrencyInstance();
    
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
        
        Statement iStmt = conn.createStatement();
        Statement cStmt = conn.createStatement();
        Statement lStmt = conn.createStatement();
        Statement sStmt = conn.createStatement();
        Statement bUStmt = conn.createStatement();
        Statement bLStmt = conn.createStatement();
        
        String iQ = "Select * from Item where ItemId = " + itemId;
        String catQ = "Select category from ItemCategory where ItemId = " + itemId;
        String locQ = "Select l.lat, l.lon, l.locText, l.country " + 
        				" from Location l, ItemLocation i where i.ItemId = " + itemId + " and i.LocId = l.LocId";
        String sellQ = "Select SellRating, UserId from User u, Item i where i.ItemId = " + itemId +
        				" and u.UserId = i.sellId";
        String bidUQ = "Select b.time, b.amount, u.UserId, u.BidRating, l.locText, l.country" +
        		" from Bid b, User u, BidLocation bl, Location l" +
        		" where b.ItemId = " + itemId + " and b.UserId = u.UserId" +
        		" and bl.UserId = b.UserId and bl.LocId = l.LocId";;
//        		"Select b.time, b.amount, u.UserId, u.BidRating" +
//        				" from Bid b, User u" +
//        				" where b.ItemId = " + itemId + " and b.UserId = u.UserId";
////        String bidLQ = "Select l.locText, l.country" +
//						" from Bid b, BidLocation bl, Location l" +
//						" where b.ItemId = " + itemId +
//						" and bl.UserId = b.UserId and bl.LocId = l.LocId";
        
       
        
        ResultSet rs = iStmt.executeQuery(iQ);
        
        if(rs.next()){
        	ResultSet catRs = cStmt.executeQuery(catQ);
        	ResultSet locRs = lStmt.executeQuery(locQ);
        	ResultSet sellRs = sStmt.executeQuery(sellQ);
        	ResultSet bidURs = bUStmt.executeQuery(bidUQ);
//        	ResultSet bidLRs = bLStmt.executeQuery(bidLQ);
        	res += "<Item ItemId=\"" + rs.getLong("ItemId") +"\">\n";
        	res += "  <Name>" + xmlFormatted(rs.getString("name")) +"</Name>\n";
        	res += catXML(catRs);
        	res += "  <Currently>" + fmt.format(rs.getDouble("currentBid")) + "</Currently>\n";
        	if(rs.getDouble("buyout") != 0){
        		res += "  <Buy_Price>" + fmt.format(rs.getDouble("buyout")) + "</Buy_Price>\n"; 
        	}
        	res += "  <First_Bid>" + fmt.format(rs.getDouble("minBid")) + "</First_Bid>\n";
        	res += "  <Number_of_Bids>" + rs.getInt("numBids") + "</Number_of_Bids>\n";
        	res += bidXML(bidURs);
        	res += locXML(locRs);
        	
        	Date sTime = null, eTime = null;
        	String stString = null, etString = null;
			try {
				sTime = inFormatter.parse(rs.getTimestamp("startTime").toString());
				eTime = inFormatter.parse(rs.getTimestamp("endTime").toString());
			} catch (java.text.ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			stString = outFormatter.format(sTime);
			etString = outFormatter.format(eTime);
        	
        	res += "  <Started>" + stString + "</Started>\n";
            res += "  <Ends>" + etString + "</Ends>\n";
            res += sellXML(sellRs);
            res += "  <Description>" + xmlFormatted(rs.getString("description")) + "</Description>\n";
            res += "</Item>\n";
        }
        
        // close the database connection
        try {
            conn.close();
        } catch (SQLException ex) {
            System.out.println(ex);
        }
        
		return res;
	}
	
	
	private String catXML(ResultSet rs) throws SQLException{
		String res = "";
		while(rs.next()){
			res += "  <Category>"+xmlFormatted(rs.getString("category"))+"</Category>\n";
		}
		return res;
	}
	private String bidXML(ResultSet rsU) throws SQLException{
		String res = "";
		if(rsU.next()){
			res += "  <Bids>\n";
		} else{
			return "  <Bids />\n";
		}
		do {
			res += "    <Bid>\n";
			res += "      <Bidder Rating=\"" + rsU.getInt("BidRating") + 
					"\" UserId=\"" + rsU.getString("UserId") + "\">\n";
			res += "        <Location>" + xmlFormatted(rsU.getString("locText")) + "</Location>\n";
			res += "        <Country>" + rsU.getString("country") + "</Country>\n";
			res += "      </Bidder>\n";
			
			Date time = null;
			try {
				time = inFormatter.parse(rsU.getTimestamp("time").toString());
			} catch (java.text.ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String outTime = outFormatter.format(time);
			
			res += "      <Time>" + outTime + "</Time>\n";
			res += "      <Amount>" + fmt.format(rsU.getDouble("amount")) + "</Amount>\n";
			res += "    </Bid>\n";
		} while(rsU.next());
		res += "  </Bids>\n";
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
			res += " Longitude=\"" + rs.getFloat("lon") + "\"";
		}
		res += ">"+xmlFormatted(rs.getString("locText")) + "</Location>\n";
		res += "  <Country>" + rs.getString("country") + "</Country>\n"; 
		return res;
	}
	private String sellXML(ResultSet rs) throws SQLException{
		String res = "";
		rs.next();
		res += "  <Seller Rating=\"" + rs.getString("SellRating") + "\" UserID=\"" + rs.getString("UserId") + "\" />\n";
		return res;
	}

	private String xmlFormatted(String input){
		input = input.replace("&", "&amp;");
		input = input.replace("'", "&apos;");
		input = input.replace("<", "&lt;");
		input = input.replace(">", "&gt;");
		input = input.replace("\"", "&quot;");
		return input;
	}
	
	public String echo(String message) {
		return message;
	}

}
