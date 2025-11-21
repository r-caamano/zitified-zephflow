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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Future;

public class FleakFlowRunner {

    public static void main(String[] args) throws Exception {
        if(!(args.length >= 3)){
            System.out.println("Not enough are arguments " + args.length + " given but 3 are required");
            System.out.println("Usage: required args = <ziti identity json> <inbound ziti service name> <outbound ziti url>");
            System.exit(1);
        }
        // Enable Ziti debug logging and SLF4J verbosity
        //System.setProperty("org.openziti.level", "DEBUG");
        //System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");

        // Load Ziti identity and initialize context
        ZitiContext ziti = Ziti.newContext(args[0], new char[]{});
        
        // Create Ziti server socket and bind to service 'FLEAK_TEST'
        ZitiServerSocketChannel server = new ZitiServerSocketChannel((ZitiContextImpl) ziti);
        server.bind(new ZitiAddress.Bind(args[1]), 0);

        // Allow time for terminator registration and edge-router propagation
        Thread.sleep(20000);
        System.out.println(" FleakFlowRunner is actively listening on Ziti service '" + args[1] + "'...");

        // Accept incoming connections indefinitely
        while (true) {
            try {
                // Wait for client connection
                Future<AsynchronousSocketChannel> futureClient = server.accept();
                AsynchronousSocketChannel client = futureClient.get();
                System.out.println("Connection accepted");
                String jobId = "job-" + client.hashCode();
                Path inputFile = Paths.get(jobId + "-input.json");
                // Read input from client to temp file
                try(InputStream input = Channels.newInputStream(client);
                    OutputStream output = Files.newOutputStream(inputFile)){
                    System.out.println("Writing input stream to file: " + inputFile.getFileName());
                    input.transferTo(output);
                }

                // Capture stdout and redirect stdin for ZephFlow
                ByteArrayOutputStream outputCapture = new ByteArrayOutputStream();
                PrintStream originalOut = System.out;
                System.setOut(new PrintStream(outputCapture));
                //close client
                client.close();

                // Start ZephFlow pipeline
                ZephFlow flow = ZephFlow.startFlow();
                //Read input from file
                ZephFlow inputFlow = flow.fileSource(inputFile.toString(),EncodingType.JSON_ARRAY);
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
                outputFlow.execute(Objects.requireNonNull(ziti.getId()).getId(), args[2], args[1]);
                long end = System.currentTimeMillis();
                System.out.println("Discarding temp file: " + inputFile.getFileName());
                Files.deleteIfExists(inputFile);
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
                                insertIntoPostgres(obj, args[2]);
                            }
                        } else if (trimmed.startsWith("{")) {
                            System.out.println("Line has {");
                            JSONObject obj = new JSONObject(trimmed);
                            insertIntoPostgres(obj, args[2]);
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
    private static Connection connectToPostgres(String service) throws SQLException {
        String jdbcUrl = "jdbc:postgresql://" + service + "/mydb";
        Properties props = new Properties();
        props.setProperty("user", "myuser");
        props.setProperty("password", "mypassword");
        props.setProperty("socketFactory", "org.openziti.net.ZitiSocketFactory"); // Ziti-aware JDBC
        System.out.println("Attempting to return Driver");
        return DriverManager.getConnection(jdbcUrl, props);
    }

    // Insert transformed JSON into Postgres
    private static void insertIntoPostgres(JSONObject obj, String service) throws SQLException {
        try (Connection conn = connectToPostgres(service)) {
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
