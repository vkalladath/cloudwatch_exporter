package io.prometheus.cloudwatch;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.logging.Logger;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;

import io.prometheus.cloudwatch.servlet.CouldWatchMetricsServlet;

public class WebServer {
	private static Logger log = Logger.getLogger(WebServer.class.getName());

    private static final String CONFIG_FILE = "CONFIG_FILE";
    private static final String CONSUL_SERVERS = "CONSUL_SERVERS";
    public static String REQUEST_TEMPLATE =
            "{" + 
            "   \"ID\": \"CloudWatchExporter-{HOST}-{PORT}\"," + 
            "   \"Name\": \"CloudWatchExporter\"," + 
            "   \"Tags\": [" + 
            "       \"CloudWatchExporter\"" + 
            "   ]," + 
            "   \"Address\": \"{HOST}\"," + 
            "   \"Port\": {PORT}," + 
            "   \"Check\": {" + 
            "       \"DeregisterCriticalServiceAfter\": \"10m\"," + 
            "       \"HTTP\": \"http://{HOST}:{PORT}\"," + 
            "       \"Interval\": \"30s\"," + 
            "       \"Status\": \"passing\"" + 
            "   }" + 
            "}";
    public static String configFilePath;

    public static void main(String[] args) throws Exception {
        
        String configFile = System.getenv(CONFIG_FILE);
        if(configFile != null && configFile.length() > 2) {
            configFilePath = configFile;
            if (args.length < 1) {
                System.err.println("Usage: WebServer <port>");
                System.exit(1);
            }
        } else if (args.length < 2) {
            System.err.println("Usage: WebServer <port> <yml configuration file>");
            System.exit(1);
        } else {
            configFilePath = args[1];
        }
        
        // get consul config
        String consulServers = System.getenv(CONSUL_SERVERS);
        if (consulServers != null) {
            registerOnConsul(consulServers);
        }

        
        CloudWatchCollector collector = new CloudWatchCollector(getConfigFileReader(configFilePath));

        ReloadSignalHandler.start(collector);

        int port = Integer.parseInt(args[0]);
        Server server = new Server(port);
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        server.setHandler(context);
        context.addServlet(new ServletHolder(new CouldWatchMetricsServlet(collector)), "/metrics/*");
        context.addServlet(new ServletHolder(new DynamicReloadServlet(collector)), "/-/reload");
        context.addServlet(new ServletHolder(new HomePageServlet()), "/");
        server.start();
        server.join();
    }

    private static Reader getConfigFileReader(String configFilePath) throws IOException {
        URL configURL = null;
        try {
            configURL = new URL(configFilePath);
        } catch (MalformedURLException e) {
            configURL = new File(configFilePath).toURI().toURL();
        }
        URLConnection connection = configURL.openConnection();
        BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));

        return in;
    }

    private static void registerOnConsul(String consulServers) {
        for (String consulServer:consulServers.split(",")) {
            String url = consulServer + "/v1/agent/service/register";
            log.info(url);
            String body = REQUEST_TEMPLATE.replace("{HOST}", System.getenv("HOST"))
                    .replace("{PORT}", System.getenv("PORT"));
            HttpResponse<String> response = null;
            try {
                response = Unirest.post(url)
                        .header("Authorization", "Basic Y2NwOjJWU1J7Z3hbMjd+")
                        .body(body)
                        .asString();
                log.info("Consul Response Status: " +response.getStatus());
                if(response.getStatus() == 200) {
                    break;
                }
            } catch (Exception e) {
            }
        }
    }
}

