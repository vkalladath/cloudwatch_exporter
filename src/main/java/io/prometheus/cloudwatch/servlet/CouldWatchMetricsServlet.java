package io.prometheus.cloudwatch.servlet;

import io.prometheus.client.exporter.common.TextFormat;
import io.prometheus.cloudwatch.CloudWatchCollector;

import java.io.IOException;
import java.io.Writer;
import java.util.Collections;
import java.util.Enumeration;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class CouldWatchMetricsServlet extends HttpServlet {

    private CloudWatchCollector collector;

    public CouldWatchMetricsServlet(CloudWatchCollector collector) {
        this.collector = collector;
    }

    @Override
    protected void doGet(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setContentType(TextFormat.CONTENT_TYPE_004);
        String namespace = req.getParameter("namespace");
        Writer writer = resp.getWriter();
        try {
            TextFormat.write004(writer, Collections.enumeration(collector.collect(namespace)));
            writer.flush();
        } finally {
            writer.close();
        }
    }

    @Override
    protected void doPost(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        doGet(req, resp);
    }
}
