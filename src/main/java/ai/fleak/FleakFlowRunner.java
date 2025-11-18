package ai.fleak;

// Ziti SDK imports for identity, context, and socket binding
import org.openziti.Ziti;
import org.openziti.ZitiContext;
import org.openziti.ZitiAddress;
import org.openziti.net.ZitiServerSocketChannel;
import org.openziti.impl.ZitiContextImpl;

// ZephFlow SDK imports for flow execution and JSON transformation
import io.fleak.zephflow.sdk.ZephFlow;
import io.fleak.zephflow.lib.serdes.EncodingType;

import org.json.JSONObject;
import org.json.JSONArray;

import java.io.*;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.Channels;
import java.sql.*;
import java.util.Properties;
import java.util.concurrent.Future;

public class FleakFlowRunner {

    public static void main(String[] args) throws Exception {
        // Enable Ziti debug logging and SLF4J verbosity
        System.setProperty("org.openziti.level", "DEBUG");
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");

        // Load Ziti identity and initialize context
        ZitiContext ziti = Ziti.newContext("fleak.json", new char[]{});


        // Create Ziti server socket and bind to service 'FLEAK_TEST'
        ZitiServerSocketChannel server = new ZitiServerSocketChannel((ZitiContextImpl) ziti);
        server.bind(new ZitiAddress.Bind("FLEAK_TEST"), 0);

        // Allow time for terminator registration and edge-router propagation
        Thread.sleep(20000);
        System.out.println(" FleakFlowRunner is actively listening on Ziti service 'FLEAK_TEST'...");

        // Accept incoming connections indefinitely
        while (true) {
            try {
                // Wait for client connection
                Future<AsynchronousSocketChannel> futureClient = server.accept();
                AsynchronousSocketChannel client = futureClient.get();
                System.out.println("Connection accepted");

                // Read input from client
                InputStream input = Channels.newInputStream(client);
                BufferedReader reader = new BufferedReader(new InputStreamReader(input));
                String rawInput = reader.readLine();

                // Skip if input is empty
                if (rawInput == null || rawInput.trim().isEmpty()) {
                    System.out.println("No input received — skipping flow execution.");
                    client.close();
                    continue;
                }

                System.out.println("Received raw input: " + rawInput);

                // Capture stdout and redirect stdin for ZephFlow
                ByteArrayOutputStream outputCapture = new ByteArrayOutputStream();
                PrintStream originalOut = System.out;
                System.setOut(new PrintStream(outputCapture));
                System.setIn(new ByteArrayInputStream(rawInput.getBytes()));

                // Start ZephFlow pipeline
                ZephFlow flow = ZephFlow.startFlow();
                ZephFlow inputFlow = flow.stdinSource(EncodingType.JSON_ARRAY);

                // 🔧 Transform input JSON
                ZephFlow transformedFlow = inputFlow.eval(
                    "dict(" +
                    "  original_value = $.value," +
                    "  doubled_value = $.value * 2," +
                    "  status = 'processed'," +
                    "  timestamp = epoch_to_ts_str($.timestamp, \"yyyy-MM-dd HH:mm:ss\")" +
                    ")"
                );

                // Prepare the flow to emit the transformed JSON to stdout upon execution
                ZephFlow outputFlow = transformedFlow.stdoutSink(EncodingType.JSON_OBJECT);

                // Execute flow and measure latency
                long start = System.currentTimeMillis();
                System.out.println("Executing flow...");
                outputFlow.execute("ziti_id", "ziti_env", "ziti_service");
                long end = System.currentTimeMillis();

                // Restore original stdout
                System.setOut(originalOut);
                System.out.println("Flow executed in " + (end - start) + "ms");
                System.out.println("Raw output:\n" + outputCapture.toString());

                // Parse output lines
                String[] lines = outputCapture.toString().split("\\R");
                System.out.println(" Line count: " + lines.length);
                for (int i = 0; i < lines.length; i++) {
                    System.out.println("Line " + i + ": [" + lines[i] + "]");
                }

                // Parse and insert each JSON line into Postgres
                for (String json : lines) {
                    try {
                        String trimmed = json.trim();
                        if (trimmed.startsWith("[")) {
                            JSONArray arr = new JSONArray(trimmed);
                            for (int i = 0; i < arr.length(); i++) {
                                JSONObject obj = arr.getJSONObject(i);
                                insertIntoPostgres(obj);
                            }
                        } else if (trimmed.startsWith("{")) {
                            System.out.println("Line has {");
                            JSONObject obj = new JSONObject(trimmed);
                            insertIntoPostgres(obj);
                        } else {
                            System.err.println("Skipping non-JSON line: " + trimmed);
                        }
                    } catch (Exception e) {
                        System.err.println("Failed to parse or insert JSON: " + json);
                        e.printStackTrace();
                    }
                }

                System.out.println("Output:\n" + outputCapture.toString());
                client.close();

            } catch (Exception e) {
                System.err.println("Error handling connection: " + e.getMessage());
                e.printStackTrace();
                Thread.sleep(1000); // Backoff before retry
            }
        }
    }

    // 🛠️ Connect to Postgres via Ziti service
    private static Connection connectToPostgres() throws SQLException {
        String jdbcUrl = "jdbc:postgresql://zitified-pg:5432/mydb";
        Properties props = new Properties();
        props.setProperty("user", "myuser");
        props.setProperty("password", "mypassword");
        props.setProperty("socketFactory", "org.openziti.net.ZitiSocketFactory"); // Ziti-aware JDBC
        System.out.println("Attempting to return Driver");
        return DriverManager.getConnection(jdbcUrl, props);
    }

    // Insert transformed JSON into Postgres
    private static void insertIntoPostgres(JSONObject obj) throws SQLException {
        try (Connection conn = connectToPostgres()) {
            System.out.println("Returned Driver");
            PreparedStatement stmt = conn.prepareStatement(
                "INSERT INTO transform (doubled_value, original_value, status, timestamp) VALUES (?, ?, ?, ?)"
            );
            stmt.setInt(1, obj.getInt("doubled_value"));
            stmt.setInt(2, obj.getInt("original_value"));
            stmt.setString(3, obj.getString("status"));
            stmt.setTimestamp(4, Timestamp.valueOf(obj.getString("timestamp")));
            System.out.printf("Inserting: doubled=%d, original=%d, status=%s, timestamp=%s%n",
                obj.getInt("doubled_value"),
                obj.getInt("original_value"),
                obj.getString("status"),
                obj.getString("timestamp"));
            stmt.executeUpdate();
            System.out.println("Insert executed.");
            stmt.close();
        } catch (Exception e) {
            System.err.println("Failed insert JSON: ");
            e.printStackTrace();
        }
    }
}
