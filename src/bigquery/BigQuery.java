package bigquery;

import java.io.IOException;
import java.util.List;
import java.util.Scanner;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;

public class BigQuery {

	    public static void main(String[] args) throws IOException {
		    
	    	Scanner scanprojectid;
		    if (args.length == 0) {
		      System.out.print("Enter the project ID : ");
		      scanprojectid = new Scanner(System.in);
		    } else {
		      scanprojectid = new Scanner(args[0]);
		    }
		 
		    String project_ID = scanprojectid.nextLine();
		    String queSQL;
		    
		    HttpTransport trans = new NetHttpTransport();
		    JsonFactory jsonfac = new JacksonFactory();
		    GoogleCredential googleCredentials = GoogleCredential.getApplicationDefault(trans, jsonfac);

		    if (googleCredentials.createScopedRequired()) {
		      googleCredentials = googleCredentials.createScoped(BigqueryScopes.all());
		    }
		    Bigquery objectBigQuery = new Bigquery.Builder(trans, jsonfac, googleCredentials)
		        .setApplicationName("Bigquery Samples").build();
		    
		    queSQL= "SELECT location as Location,restaurant as Restaurant,street as Street,freeway as Freeway,imageindex as "
		    		+ "Image_Index_Reference  FROM [BigTableDemo.GoogleMapData]";
          
		    //SELECT TOP(location, 20) as Location, COUNT(*) as unique_words FROM [BigTableDemo.GoogleMapData]
		    // SELECT location as Location,restaurant as Restaurant,street as Street,freeway as Freeway,imageindex as 
		    //Image_Index_Reference  FROM [BigTableDemo.GoogleMapData] 
		    
		    
		    QueryResponse queryResponse = objectBigQuery.jobs().query(project_ID,new QueryRequest().setQuery(queSQL)).execute();
     	    GetQueryResultsResponse querydataset = objectBigQuery.jobs().getQueryResults( queryResponse.getJobReference().getProjectId(),
			        queryResponse.getJobReference().getJobId()).execute();

		    List<TableRow> tableRows= querydataset.getRows();
		    System.out.print("\nBigTable Demo. Retrived Data:\n------------\n");

	    	for (TableRow tableRow : tableRows) {
		    	for (TableCell field : tableRow.getF()) {
		        System.out.printf("%-28s", field.getV());
		      }
		      System.out.println();
		    }
	    	
		  }
	}
