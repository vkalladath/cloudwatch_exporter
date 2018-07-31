package io.prometheus.cloudwatch;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;

import org.apache.http.client.HttpClient;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.conn.ssl.X509HostnameVerifier;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.SingleClientConnManager;
import org.json.JSONObject;

import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

public class ESClient {
	
    private static final Logger LOGGER = Logger.getLogger(ESClient.class.getName());

    private static final String UNTAGGED = "UNTAGGED";
    private static final String TAGS_PREFIX = "tags.";
    private static final String[] TAG_NAMES = { "tags.Environment", "tags.Stack", "tags.Application", "tags.Role", "tags.WorkLoad", "accountname", "region"};

    public static void main(String[] args) throws UnirestException, KeyManagementException, NoSuchAlgorithmException, KeyStoreException {
        String fieldName = "autoscalinggroupname";
        String fieldValue = "mya-mesos-spark-test-01-MesosSparkAsg-172ZF4T9G49GO";
        String esPath = "aws/classicelb";
        Map tags = findTagsForResource(fieldName, fieldValue, esPath, null);
        // System.out.println(tags);
    }

    public static Map<String, String> findTagsForResource(String fieldName, String fieldValue, String esPath, List<String> additionalLabels) {
        Map<String, String> tags = defaultEmptyTags(TAG_NAMES, additionalLabels);
        Unirest.setHttpClient(makeClient());

        String sb = buildQuery(fieldName.toLowerCase(), fieldValue);
        JsonNode node;
        try {
        	LOGGER.info("## Connecting to ES " + fieldValue + " " + fieldName);
            node = Unirest.post("https://search.pacman.corporate.t-mobile.com/api/console/proxy?path=" + esPath.toLowerCase() + "/_search&method=POST").header("kbn-xsrf", "1")
                    .body(sb).asJson().getBody();
        } catch (UnirestException e) {
            // TODO Log Error
        	LOGGER.warning("Error connecting to Pacman API: " + e.getMessage());
            return tags;
        }
        int totalResults = node.getObject().getJSONObject("hits").getInt("total");
        LOGGER.fine("fieldName: " + fieldName + " fieldValue: " + fieldValue + " esPath: " +esPath);
        if (totalResults == 1) {
            JSONObject source = node.getObject().getJSONObject("hits").getJSONArray("hits").getJSONObject(0).getJSONObject("_source");
            // Tag names are case sensitive
            
            if(additionalLabels != null && additionalLabels.size() > 0) {
                extractTags(tags, source, additionalLabels.toArray(new String[additionalLabels.size()]));
            }
            extractTags(tags, source, TAG_NAMES);
        } else if (totalResults == 0) {
            // TODO: warning
        } else {
            // TODO: error
        }
        return tags;
    }

	private static HashMap<String, String> defaultEmptyTags(String[] tagNames, List<String> additionalLabels) {
		HashMap<String, String> tags = new HashMap<String, String>();
		for (String tagName : tagNames) {
			tags.put(sanitizeTagName(tagName), UNTAGGED);
		}
		if(additionalLabels != null && additionalLabels.size() > 0) {
			for (String tagName : additionalLabels) {
				tags.put(sanitizeTagName(tagName), UNTAGGED);
			}
		}
		return tags;
	}

    private static void extractTags(Map<String, String> tags, JSONObject source, String[] tagNames) {
        for (String tagName : tagNames) {
            if(source.has(tagName)){
                String tagValue = source.getString(tagName);
                tags.put(sanitizeTagName(tagName), tagValue);
            } else {
                tags.put(sanitizeTagName(tagName), UNTAGGED);
            }
        }
    }

    private static String sanitizeTagName(String tagName) {
        if(tagName.startsWith(TAGS_PREFIX)) {
            tagName = tagName.substring(TAGS_PREFIX.length());
        }
        return tagName.toLowerCase();
    }

    private static String buildQuery(String fieldName, String fieldValue) {
        return "{\"size\":\"2\",\"query\":{\"bool\":{\"must\":[{\"term\":{\"" + fieldName + ".keyword\":\"" + fieldValue + "\"}},{\"match\":{\"latest\":true}}]}}}";
    }

    public static HttpClient makeClient() {
        SchemeRegistry schemeRegistry = new SchemeRegistry();
        schemeRegistry.register(new Scheme("http", 80, PlainSocketFactory.getSocketFactory()));
        try {
            schemeRegistry.register(new Scheme("https", 443, new MockSSLSocketFactory()));
        } catch (KeyManagementException e) {
            e.printStackTrace();
        } catch (UnrecoverableKeyException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (KeyStoreException e) {
            e.printStackTrace();
        }
        ClientConnectionManager cm = new SingleClientConnManager(schemeRegistry);
        DefaultHttpClient httpclient = new DefaultHttpClient(cm);
        return httpclient;
    }

}

class MockSSLSocketFactory extends SSLSocketFactory {

    public MockSSLSocketFactory() throws NoSuchAlgorithmException, KeyManagementException, KeyStoreException, UnrecoverableKeyException {
        super(trustStrategy, hostnameVerifier);
    }

    private static final X509HostnameVerifier hostnameVerifier = new X509HostnameVerifier() {
        // @Override
        public void verify(String host, SSLSocket ssl) throws IOException {
            // Do nothing
        }

        // @Override
        public void verify(String host, X509Certificate cert) throws SSLException {
            // Do nothing
        }

        // @Override
        public void verify(String host, String[] cns, String[] subjectAlts) throws SSLException {
            // Do nothing
        }

        // @Override
        public boolean verify(String s, SSLSession sslSession) {
            return true;
        }
    };
    private static final TrustStrategy trustStrategy = new TrustStrategy() {
        // @Override
        public boolean isTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            return true;
        }
    };
}