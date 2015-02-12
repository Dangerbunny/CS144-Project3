package edu.ucla.cs.cs144;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;

import org.apache.lucene.queryparser.classic.ParseException;

import edu.ucla.cs.cs144.AuctionSearch;
import edu.ucla.cs.cs144.SearchRegion;
import edu.ucla.cs.cs144.SearchResult;

public class AuctionSearchTest {
	public static void main(String[] args1)
	{
		AuctionSearch as = new AuctionSearch();

		String message = "Test message";
		String reply = as.echo(message);
		System.out.println("Reply: " + reply);
		
		String query = "superman";
		SearchResult[] basicResults = null;
		try {
			basicResults = as.basicSearch(query, 0, 20);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Basic Seacrh Query: " + query);
		System.out.println("Received " + basicResults.length + " results");
		for(SearchResult result : basicResults) {
			System.out.println(result.getItemId() + ": " + result.getName());
		}
		
		SearchRegion region =
		    new SearchRegion(33.774, -118.63, 34.201, -117.38); 
		SearchResult[] spatialResults = null;
		try {
			spatialResults = as.spatialSearch("camera", region, 0, 20);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Spatial Seacrh");
		System.out.println("Received " + spatialResults.length + " results");
		for(SearchResult result : spatialResults) {
			System.out.println(result.getItemId() + ": " + result.getName());
		}
		
		String itemId = "1043495702";
		String item = null;
		try {
			item = as.getXMLDataForItemId(itemId);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("XML data for ItemId: " + itemId);
		System.out.println(item);

		// Add your own test here
	}
}
