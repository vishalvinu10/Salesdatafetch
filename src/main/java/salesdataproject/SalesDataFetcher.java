package salesdataproject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class SalesDataFetcher {

    // Environment variables with fallback values
    private static final String APP_KEY = System.getenv("APP_KEY") != null ? System.getenv("APP_KEY") : "default_app_key";
    private static final String APP_SECRET = System.getenv("APP_SECRET") != null ? System.getenv("APP_SECRET") : "default_app_secret";
    private static final String ACCESS_TOKEN = System.getenv("ACCESS_TOKEN") != null ? System.getenv("ACCESS_TOKEN") : "default_access_token";
    private static final String REST_ID = System.getenv("REST_ID") != null ? System.getenv("REST_ID") : "default_rest_id";
    private static final String FROM_DATE = "2025-01-20 00:00:00";
    private static final String TO_DATE = "2025-01-20 23:59:59";

    // API endpoint with URL encoding
    private static final String API_URL = String.format(
            "http://api.petpooja.com/V1/orders/get_sales_data/?app_key=%s&app_secret=%s&access_token=%s&restID=%s&from_date=%s&to_date=%s",
            APP_KEY, APP_SECRET, ACCESS_TOKEN, REST_ID,
            URLEncoder.encode(FROM_DATE, StandardCharsets.UTF_8),
            URLEncoder.encode(TO_DATE, StandardCharsets.UTF_8));

    // Fetch sales data from API with retry mechanism
    public static JsonNode getSalesData(String apiUrl, int retries) throws IOException, InterruptedException {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        for (int attempt = 0; attempt < retries; attempt++) {
            try {
                HttpGet request = new HttpGet(apiUrl);
                HttpResponse response = httpClient.execute(request);
                if (response.getStatusLine().getStatusCode() == 200) {
                    String jsonResponse = EntityUtils.toString(response.getEntity());
                    ObjectMapper mapper = new ObjectMapper();
                    return mapper.readTree(jsonResponse);
                } else {
                    System.err.printf("Attempt %d failed: HTTP %d%n", attempt + 1, response.getStatusLine().getStatusCode());
                }
            } catch (IOException e) {
                System.err.printf("Attempt %d failed: %s%n", attempt + 1, e.getMessage());
            }
            Thread.sleep(2000); 
        }
        return null;
    }

    // Parse and store sales data in SQLite
    public static void storeSalesData(JsonNode data, String dbName) {
        Connection conn = null;
        try {
            // Connect to SQLite database
            conn = DriverManager.getConnection("jdbc:sqlite:" + dbName);

            // Disable auto-commit mode
            conn.setAutoCommit(false);

            Statement stmt = conn.createStatement();

            // Create sales_data table
            stmt.execute("CREATE TABLE IF NOT EXISTS sales_data (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    "receipt_number TEXT, " +
                    "sale_date TEXT, " +
                    "transaction_time TEXT, " +
                    "sale_amount REAL, " +
                    "tax_amount REAL, " +
                    "discount_amount REAL, " +
                    "round_off REAL, " +
                    "net_sale REAL, " +
                    "payment_mode TEXT, " +
                    "order_type TEXT, " +
                    "transaction_status TEXT)");

            // Insert sales data into table
            PreparedStatement pstmt = conn.prepareStatement(
                    "INSERT INTO sales_data (receipt_number, sale_date, transaction_time, sale_amount, " +
                            "tax_amount, discount_amount, round_off, net_sale, payment_mode, order_type, transaction_status) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

            JsonNode records = data.path("data");
            for (JsonNode record : records) {
                pstmt.setString(1, record.path("receipt_number").asText());
                pstmt.setString(2, record.path("sale_date").asText());
                pstmt.setString(3, record.path("transaction_time").asText("00:00:00"));
                pstmt.setDouble(4, record.path("invoice_amount").asDouble(0.0));
                pstmt.setDouble(5, record.path("tax_amount").asDouble(0.0));
                pstmt.setDouble(6, record.path("discount_amount").asDouble(0.0));
                pstmt.setDouble(7, record.path("round_off").asDouble(0.0));
                pstmt.setDouble(8, record.path("net_sale").asDouble(0.0));
                pstmt.setString(9, record.path("payment_mode").asText("Unknown"));
                pstmt.setString(10, record.path("order_type").asText("Unknown"));
                pstmt.setString(11, record.path("transaction_status").asText("Unknown"));
                pstmt.executeUpdate();
            }

           
            conn.commit();
            System.out.println("Data successfully inserted into the database.");
        } catch (SQLException e) {
            System.err.println("SQL Error: " + e.getMessage());
            try {
                // Rollback the transaction in case of an error
                if (conn != null) {
                    conn.rollback();
                }
            } catch (SQLException ex) {
                System.err.println("Rollback Error: " + ex.getMessage());
            }
        } finally {
            // Close the connection
            if (conn != null) {
                try {
                    conn.setAutoCommit(true); // Re-enable auto-commit mode
                    conn.close();
                } catch (SQLException e) {
                    System.err.println("Error closing connection: " + e.getMessage());
                }
            }
        }
    }


    public static void main(String[] args) {
        try {
            // Debug Print environment variables
            System.out.println("APP_KEY: " + APP_KEY);
            System.out.println("APP_SECRET: " + APP_SECRET);
            System.out.println("ACCESS_TOKEN: " + ACCESS_TOKEN);
            System.out.println("REST_ID: " + REST_ID);

            // Fetch data from API
            JsonNode data = getSalesData(API_URL, 3);
            if (data != null) {
                System.out.println("Data fetched successfully from API.");
                storeSalesData(data, "sales_data.db");
            } else {
                System.out.println("Failed to fetch sales data after multiple attempts.");
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}