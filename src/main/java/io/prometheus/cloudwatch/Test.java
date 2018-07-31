package io.prometheus.cloudwatch;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

public class Test {
    public static void main(String[] args) throws IOException {
        Reader r = getConfigFileReader("D:/Dev/ccpWokspace/cloudwatch_exporter/example.yml");
        // "https://www.sample-videos.com/text/Sample-text-file-10kb.txt");//
        BufferedReader br = new BufferedReader(r);

        String sCurrentLine;

        while ((sCurrentLine = br.readLine()) != null) {
            System.out.println(sCurrentLine);
        }

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
}
