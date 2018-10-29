package io.prometheus.cloudwatch;

import io.prometheus.client.Collector;
import io.prometheus.client.Counter;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.yaml.snakeyaml.Yaml;

import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.DimensionFilter;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.amazonaws.services.cloudwatch.model.ListMetricsRequest;
import com.amazonaws.services.cloudwatch.model.ListMetricsResult;
import com.amazonaws.services.cloudwatch.model.Metric;

public class CloudWatchCollector extends Collector {

    private static final String METRICS_CACHE = "metrics";

    private static final String DIMENSIONS_CACHE = "dimensions";

    private static final String ES_CACHE = "es";

    private static final Logger LOGGER = Logger.getLogger(CloudWatchCollector.class.getName());

    static class ActiveConfig implements Cloneable {
        ArrayList<MetricRule> rules;
        AmazonCloudWatchClient client;
        Map<String, ResourceMapping> mappings;

        @Override
        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }
    }
    
    static class ResourceMapping {
    	String resourceType;
    	String resourceIDField;
    	String esResourceIDField;
    	String lookupURL;
    	List<String> additionalLabels;
    }
    
    static class MetricRule {
      String awsNamespace;
      String awsMetricName;
      int periodSeconds;
      int rangeSeconds;
      int delaySeconds;
      List<String> awsStatistics;
      List<String> awsExtendedStatistics;
      List<String> awsDimensions;
      Map<String,List<String>> awsDimensionSelect;
      Map<String,List<String>> awsDimensionSelectRegex;
      String help;
    }

    ActiveConfig activeConfig = new ActiveConfig();

    static{
        CacheProvider.initCache(ES_CACHE, 100000, 6 * 3600); // 6 hours
        CacheProvider.initCache(DIMENSIONS_CACHE, 500, 4 * 3600); // 4 hours
        CacheProvider.initCache(METRICS_CACHE, 1000000, 2 * 60); // 2 minutes
    }
    
    private static final Counter cloudwatchRequests = Counter.build()
      .name("cloudwatch_requests_total").help("API requests made to CloudWatch").register();

    private static final List<String> brokenDynamoMetrics = Arrays.asList(
            "ConsumedReadCapacityUnits", "ConsumedWriteCapacityUnits",
            "ProvisionedReadCapacityUnits", "ProvisionedWriteCapacityUnits",
            "ReadThrottleEvents", "WriteThrottleEvents");

    public CloudWatchCollector(Reader in) throws IOException {
        loadConfig(in, null);
    }
    public CloudWatchCollector(String yamlConfig) {
        this((Map<String, Object>)new Yaml().load(yamlConfig),null);
    }

    /* For unittests. */
    protected CloudWatchCollector(String jsonConfig, AmazonCloudWatchClient client) {
        this((Map<String, Object>)new Yaml().load(jsonConfig), client);
    }

    private CloudWatchCollector(Map<String, Object> config, AmazonCloudWatchClient client) {
        loadConfig(config, client);
    }

    protected void reloadConfig() throws IOException {
        LOGGER.log(Level.INFO, "Reloading configuration");

        loadConfig(new FileReader(WebServer.configFilePath), activeConfig.client);
    }

    protected void loadConfig(Reader in, AmazonCloudWatchClient client) throws IOException {
        loadConfig((Map<String, Object>)new Yaml().load(in), client);
    }
    private void loadConfig(Map<String, Object> config, AmazonCloudWatchClient client) {

        if(config == null) {  // Yaml config empty, set config to empty map.
            config = new HashMap<String, Object>();
        }
        if (!config.containsKey("region")) {
          throw new IllegalArgumentException("Must provide region");
        }

        int defaultPeriod = 60;
        if (config.containsKey("period_seconds")) {
          defaultPeriod = ((Number)config.get("period_seconds")).intValue();
        }
        int defaultRange = 120;
        if (config.containsKey("range_seconds")) {
          defaultRange = ((Number)config.get("range_seconds")).intValue();
        }
        int defaultDelay = 60;
        if (config.containsKey("delay_seconds")) {
          defaultDelay = ((Number)config.get("delay_seconds")).intValue();
        }

        if (client == null) {
          if (config.containsKey("role_arn")) {
            STSAssumeRoleSessionCredentialsProvider credentialsProvider = new STSAssumeRoleSessionCredentialsProvider(
              (String) config.get("role_arn"),
              "cloudwatch_exporter"
            );
            client = new AmazonCloudWatchClient(credentialsProvider);
          } else {
            client = new AmazonCloudWatchClient();
          }
          Region region = RegionUtils.getRegion((String) config.get("region"));
          client.setEndpoint(getMonitoringEndpoint(region));
        }

        if (!config.containsKey("metrics")) {
          throw new IllegalArgumentException("Must provide metrics");
        }
                
        Map<String, ResourceMapping> mappings = new HashMap<String, ResourceMapping>();
        if (config.containsKey("mappings")) {
           
            for (Object ruleObject : (List<Map<String,Object>>) config.get("mappings")) {
              Map<String, Object> yamlResourceMapping = (Map<String, Object>)ruleObject;
              ResourceMapping mapping = new ResourceMapping();
              if (!yamlResourceMapping.containsKey("name") || !yamlResourceMapping.containsKey("id_field")|| !yamlResourceMapping.containsKey("lookup_url")) {
                  throw new IllegalArgumentException("Must provide aws_namespace and aws_metric_name");
              }
              mapping.resourceType = (String)yamlResourceMapping.get("name");
              mapping.resourceIDField = (String)yamlResourceMapping.get("id_field");
              mapping.lookupURL = (String)yamlResourceMapping.get("lookup_url");
              if (yamlResourceMapping.containsKey("additional_labels")) {
                  mapping.additionalLabels = (List<String>)yamlResourceMapping.get("additional_labels");
              } 
			  if (yamlResourceMapping.containsKey("es_id_field")) {
				  mapping.esResourceIDField = (String) yamlResourceMapping.get("es_id_field");
			  } else {
				  mapping.esResourceIDField = mapping.resourceIDField;
			  }

              mappings.put(mapping.resourceType, mapping);
            }
        }
        
        ArrayList<MetricRule> rules = new ArrayList<MetricRule>();
        
        for (Object ruleObject : (List<Map<String,Object>>) config.get("metrics")) {
          Map<String, Object> yamlMetricRule = (Map<String, Object>)ruleObject;
          MetricRule rule = new MetricRule();
          rules.add(rule);
          if (!yamlMetricRule.containsKey("aws_namespace") || !yamlMetricRule.containsKey("aws_metric_name")) {
            throw new IllegalArgumentException("Must provide aws_namespace and aws_metric_name");
          }
          rule.awsNamespace = (String)yamlMetricRule.get("aws_namespace");
          rule.awsMetricName = (String)yamlMetricRule.get("aws_metric_name");
          if (yamlMetricRule.containsKey("help")) {
            rule.help = (String)yamlMetricRule.get("help");
          }
          if (yamlMetricRule.containsKey("aws_dimensions")) {
            rule.awsDimensions = (List<String>)yamlMetricRule.get("aws_dimensions");
          }
          if (yamlMetricRule.containsKey("aws_dimension_select") && yamlMetricRule.containsKey("aws_dimension_select_regex")) {
            throw new IllegalArgumentException("Must not provide aws_dimension_select and aws_dimension_select_regex at the same time");
          }
          if (yamlMetricRule.containsKey("aws_dimension_select")) {
            rule.awsDimensionSelect = (Map<String, List<String>>)yamlMetricRule.get("aws_dimension_select");
          }
          if (yamlMetricRule.containsKey("aws_dimension_select_regex")) {
            rule.awsDimensionSelectRegex = (Map<String,List<String>>)yamlMetricRule.get("aws_dimension_select_regex");
          }
          if (yamlMetricRule.containsKey("aws_statistics")) {
            rule.awsStatistics = (List<String>)yamlMetricRule.get("aws_statistics");
          } else if (!yamlMetricRule.containsKey("aws_extended_statistics")) {
            rule.awsStatistics = new ArrayList(Arrays.asList("Sum", "SampleCount", "Minimum", "Maximum", "Average"));
          }
          if (yamlMetricRule.containsKey("aws_extended_statistics")) {
            rule.awsExtendedStatistics = (List<String>)yamlMetricRule.get("aws_extended_statistics");
          }
          if (yamlMetricRule.containsKey("period_seconds")) {
            rule.periodSeconds = ((Number)yamlMetricRule.get("period_seconds")).intValue();
          } else {
            rule.periodSeconds = defaultPeriod;
          }
          if (yamlMetricRule.containsKey("range_seconds")) {
            rule.rangeSeconds = ((Number)yamlMetricRule.get("range_seconds")).intValue();
          } else {
            rule.rangeSeconds = defaultRange;
          }
          if (yamlMetricRule.containsKey("delay_seconds")) {
            rule.delaySeconds = ((Number)yamlMetricRule.get("delay_seconds")).intValue();
          } else {
            rule.delaySeconds = defaultDelay;
          }
        }

        loadConfig(mappings, rules, client);
    }

    private void loadConfig(Map<String, ResourceMapping> mappings, ArrayList<MetricRule> rules, AmazonCloudWatchClient client) {
        synchronized (activeConfig) {
            activeConfig.client = client;
            activeConfig.rules = rules;
            activeConfig.mappings = mappings;
        }
    }

    public String getMonitoringEndpoint(Region region) {
      return "https://" + region.getServiceEndpoint("monitoring");
    }

    private List<List<Dimension>> getDimensions(MetricRule rule, AmazonCloudWatchClient client) {
        
      Object dimensionsFromCache = CacheProvider.getFromCache(DIMENSIONS_CACHE, generateKey(rule.awsNamespace, rule.awsMetricName));
      if (dimensionsFromCache != null) {
          return (List<List<Dimension>>) dimensionsFromCache;
      }
      List<List<Dimension>> dimensions = new ArrayList<List<Dimension>>();
      if (rule.awsDimensions == null) {
        dimensions.add(new ArrayList<Dimension>());
        return dimensions;
      }

      ListMetricsRequest request = new ListMetricsRequest();
      request.setNamespace(rule.awsNamespace);
      request.setMetricName(rule.awsMetricName);
      List<DimensionFilter> dimensionFilters = new ArrayList<DimensionFilter>();
      for (String dimension: rule.awsDimensions) {
        dimensionFilters.add(new DimensionFilter().withName(dimension));
      }
      request.setDimensions(dimensionFilters);

      String nextToken = null;
      do {
        request.setNextToken(nextToken);
        ListMetricsResult result = client.listMetrics(request);
        cloudwatchRequests.inc();
        LOGGER.log(Level.FINE, cloudwatchRequests.get() + "");
        for (Metric metric: result.getMetrics()) {
          if (metric.getDimensions().size() != dimensionFilters.size()) {
            // AWS returns all the metrics with dimensions beyond the ones we ask for,
            // so filter them out.
            continue;
          }
          if (useMetric(rule, metric)) {
            dimensions.add(metric.getDimensions());
          }
        }
        nextToken = result.getNextToken();
      } while (nextToken != null);

      CacheProvider.put(DIMENSIONS_CACHE, generateKey(rule.awsNamespace, rule.awsMetricName), dimensions);
      return dimensions;
    }

    /**
     * Check if a metric should be used according to `aws_dimension_select` or `aws_dimension_select_regex`
     */
    private boolean useMetric(MetricRule rule, Metric metric) {
      if (rule.awsDimensionSelect == null && rule.awsDimensionSelectRegex == null) {
        return true;
      }
      if (rule.awsDimensionSelect != null  && metricsIsInAwsDimensionSelect(rule, metric)) {
        return true;
      }
      if (rule.awsDimensionSelectRegex != null  && metricIsInAwsDimensionSelectRegex(rule, metric)) {
        return true;
      }
      return false;
    }

    /**
     * Check if a metric is matched in `aws_dimension_select`
     */
    private boolean metricsIsInAwsDimensionSelect(MetricRule rule, Metric metric) {
      Set<String> dimensionSelectKeys = rule.awsDimensionSelect.keySet();
      for (Dimension dimension : metric.getDimensions()) {
        String dimensionName = dimension.getName();
        String dimensionValue = dimension.getValue();
        if (dimensionSelectKeys.contains(dimensionName)) {
          List<String> allowedDimensionValues = rule.awsDimensionSelect.get(dimensionName);
          if (!allowedDimensionValues.contains(dimensionValue)) {
            return false;
          }
        }
      }
      return true;
    }

    /**
     * Check if a metric is matched in `aws_dimension_select_regex`
     */
    private boolean metricIsInAwsDimensionSelectRegex(MetricRule rule, Metric metric) {
      Set<String> dimensionSelectRegexKeys = rule.awsDimensionSelectRegex.keySet();
      for (Dimension dimension : metric.getDimensions()) {
        String dimensionName = dimension.getName();
        String dimensionValue = dimension.getValue();
        if (dimensionSelectRegexKeys.contains(dimensionName)) {
          List<String> allowedDimensionValues = rule.awsDimensionSelectRegex.get(dimensionName);
          if (!regexListMatch(allowedDimensionValues, dimensionValue)) {
            return false;
          }
        }
      }
      return true;
    }

    /**
     * Check if any regex string in a list matches a given input value
     */
    protected static boolean regexListMatch(List<String> regexList, String input) {
      for (String regex: regexList) {
        if (Pattern.matches(regex, input)) {
          return true;
        }
      }
      return false;
    }

    private Datapoint getNewestDatapoint(java.util.List<Datapoint> datapoints) {
      Datapoint newest = null;
      for (Datapoint d: datapoints) {
        if (newest == null || newest.getTimestamp().before(d.getTimestamp())) {
          newest = d;
        }
      }
      return newest;
    }

    private String toSnakeCase(String str) {
      return str.replaceAll("([a-z0-9])([A-Z])", "$1_$2").toLowerCase();
    }

    private String safeName(String s) {
      // Change invalid chars to underscore, and merge underscores.
      return s.replaceAll("[^a-zA-Z0-9:_]", "_").replaceAll("__+", "_");
    }

    private String help(MetricRule rule, String unit, String statistic) {
      if (rule.help != null) {
          return rule.help;
      }
      return "CloudWatch metric " + rule.awsNamespace + " " + rule.awsMetricName
          + " Dimensions: " + rule.awsDimensions + " Statistic: " + statistic
          + " Unit: " + unit;
    }

    private void scrape(String requestedMetricNamespace, List<MetricFamilySamples> mfs) throws CloneNotSupportedException {
      ActiveConfig config = (ActiveConfig) activeConfig.clone();

      long start = System.currentTimeMillis();
      int metricQueryCount = 0;
      for (MetricRule rule: config.rules) {
        if(requestedMetricNamespace != null && !requestedMetricNamespace.equalsIgnoreCase(rule.awsNamespace)) {
            continue;
        }
        
        Date startDate = new Date(start - 1000 * rule.delaySeconds);
        Date endDate = new Date(start - 1000 * (rule.delaySeconds + rule.rangeSeconds));
        GetMetricStatisticsRequest request = new GetMetricStatisticsRequest();
        request.setNamespace(rule.awsNamespace);
        request.setMetricName(rule.awsMetricName);
        request.setStatistics(rule.awsStatistics);
        request.setExtendedStatistics(rule.awsExtendedStatistics);
        request.setEndTime(startDate);
        request.setStartTime(endDate);
        request.setPeriod(rule.periodSeconds);

        String baseName = safeName(rule.awsNamespace.toLowerCase() + "_" + toSnakeCase(rule.awsMetricName));
        String jobName = safeName(rule.awsNamespace.toLowerCase());
        List<MetricFamilySamples.Sample> sumSamples = new ArrayList<MetricFamilySamples.Sample>();
        List<MetricFamilySamples.Sample> sampleCountSamples = new ArrayList<MetricFamilySamples.Sample>();
        List<MetricFamilySamples.Sample> minimumSamples = new ArrayList<MetricFamilySamples.Sample>();
        List<MetricFamilySamples.Sample> maximumSamples = new ArrayList<MetricFamilySamples.Sample>();
        List<MetricFamilySamples.Sample> averageSamples = new ArrayList<MetricFamilySamples.Sample>();
        HashMap<String, ArrayList<MetricFamilySamples.Sample>> extendedSamples = new HashMap<String, ArrayList<MetricFamilySamples.Sample>>();

        String unit = null;

        if (rule.awsNamespace.equals("AWS/DynamoDB")
                && rule.awsDimensions.contains("GlobalSecondaryIndexName")
                && brokenDynamoMetrics.contains(rule.awsMetricName)) {
            baseName += "_index";
        }

        for (List<Dimension> dimensions: getDimensions(rule, config.client)) {
          request.setDimensions(dimensions);
          String key = generateKey(rule.awsNamespace, rule.awsMetricName, String.join("#", rule.awsStatistics.toArray(new String[0])));
          for(Dimension dimension: dimensions){
              key = key + dimension.getName() + dimension.getValue();
          }
          Object fromCache = CacheProvider.getFromCache(METRICS_CACHE, key);
          Datapoint dp = null;
          if(fromCache != null) {
              dp = (Datapoint) fromCache;
          } else {
              //System.out.println("metricQueryCount " + ++metricQueryCount);
              GetMetricStatisticsResult result = config.client.getMetricStatistics(request);
              cloudwatchRequests.inc();
              dp = getNewestDatapoint(result.getDatapoints());
              CacheProvider.put(METRICS_CACHE, key, dp);
          }
          if (dp == null) {
            continue;
          }
          
          unit = dp.getUnit();

          List<String> labelNames = new ArrayList<String>();
          List<String> labelValues = new ArrayList<String>();
          labelNames.add("job");
          labelValues.add(jobName);
          labelNames.add("instance");
          labelValues.add("");
          for (Dimension d: dimensions) {
            labelNames.add(safeName(toSnakeCase(d.getName())));
            labelValues.add(d.getValue());
          }
          addLabelsFromAWSTags(config, rule, labelNames, labelValues);
          if (dp.getSum() != null) {
            sumSamples.add(new MetricFamilySamples.Sample(
                baseName + "_sum", labelNames, labelValues, dp.getSum()));
          }
          if (dp.getSampleCount() != null) {
            sampleCountSamples.add(new MetricFamilySamples.Sample(
                baseName + "_sample_count", labelNames, labelValues, dp.getSampleCount()));
          }
          if (dp.getMinimum() != null) {
            minimumSamples.add(new MetricFamilySamples.Sample(
                baseName + "_minimum", labelNames, labelValues, dp.getMinimum()));
          }
          if (dp.getMaximum() != null) {
            maximumSamples.add(new MetricFamilySamples.Sample(
                baseName + "_maximum",labelNames, labelValues, dp.getMaximum()));
          }
          if (dp.getAverage() != null) {
            averageSamples.add(new MetricFamilySamples.Sample(
                baseName + "_average", labelNames, labelValues, dp.getAverage()));
          }
          if (dp.getExtendedStatistics() != null) {
            for (Map.Entry<String, Double> entry : dp.getExtendedStatistics().entrySet()) {
              ArrayList<MetricFamilySamples.Sample> samples = extendedSamples.get(entry.getKey());
              if (samples == null) {
                samples = new ArrayList<MetricFamilySamples.Sample>();
                extendedSamples.put(entry.getKey(), samples);
              }
              samples.add(new MetricFamilySamples.Sample(
                  baseName + "_" + safeName(toSnakeCase(entry.getKey())), labelNames, labelValues, entry.getValue()));
            }
          }
        }

        if (!sumSamples.isEmpty()) {
          mfs.add(new MetricFamilySamples(baseName + "_sum", Type.GAUGE, help(rule, unit, "Sum"), sumSamples));
        }
        if (!sampleCountSamples.isEmpty()) {
          mfs.add(new MetricFamilySamples(baseName + "_sample_count", Type.GAUGE, help(rule, unit, "SampleCount"), sampleCountSamples));
        }
        if (!minimumSamples.isEmpty()) {
          mfs.add(new MetricFamilySamples(baseName + "_minimum", Type.GAUGE, help(rule, unit, "Minimum"), minimumSamples));
        }
        if (!maximumSamples.isEmpty()) {
          mfs.add(new MetricFamilySamples(baseName + "_maximum", Type.GAUGE, help(rule, unit, "Maximum"), maximumSamples));
        }
        if (!averageSamples.isEmpty()) {
          mfs.add(new MetricFamilySamples(baseName + "_average", Type.GAUGE, help(rule, unit, "Average"), averageSamples));
        }
        for (Map.Entry<String, ArrayList<MetricFamilySamples.Sample>> entry : extendedSamples.entrySet()) {
          mfs.add(new MetricFamilySamples(baseName + "_" + safeName(toSnakeCase(entry.getKey())), Type.GAUGE, help(rule, unit, entry.getKey()), entry.getValue()));
        }
      }
    }

    private void addLabelsFromAWSTags(ActiveConfig config, MetricRule rule, List<String> labelNames, List<String> labelValues) {
        // TODO: Currently ignores region
        // Region region = RegionUtils.getRegion((String) config.get("region"));
        
        ResourceMapping mapping = config.mappings.get(rule.awsNamespace);
        if (mapping != null) {
            String resourceName = findResourceName(labelNames, labelValues, mapping);
            Map<String, String> tags = readTagsForResource(mapping.esResourceIDField, resourceName, mapping);
            for (String key:tags.keySet()){
                
                labelNames.add(safeName(toSnakeCase(key)));
                labelValues.add(tags.get(key));
            }
        } else {
            // TODO: Log warning
            LOGGER.log(Level.WARNING, "Resource Mapping not configured - " + rule.awsNamespace);
        }
    }

    private String findResourceName(List<String> labelNames, List<String> labelValues, ResourceMapping mapping) {
        String resourceName = "";
        for (int i = 0; i < labelNames.size(); i++) {
            if (labelNames.get(i).equalsIgnoreCase(safeName(toSnakeCase(mapping.resourceIDField)))) {
            	resourceName = labelValues.get(i);
            	// special handling for Network ELB
            	if (resourceName.startsWith("net/")) {
            		resourceName = resourceName.substring(4, resourceName.indexOf("/", 4));
            		labelValues.set(i, resourceName);
            	}
            }
        }
        return resourceName;
    }

    private Map<String, String> readTagsForResource(String resourceIDField, String resourceName, ResourceMapping mapping) {
        String lookupURL = mapping.lookupURL;
        if (lookupURL == null || resourceName == null || resourceName.isEmpty()) {
            // TODO: Log error
            LOGGER.log(Level.WARNING, "Resource Name Label not found in Data from CloudWatch - " + resourceIDField);
        } else {
            Map<String, String> tags = (Map<String, String>)CacheProvider.getFromCache(ES_CACHE, generateKey(resourceIDField, resourceName, lookupURL));
            if(tags == null) {
                tags = ESClient.findTagsForResource(resourceIDField, resourceName, lookupURL, mapping.additionalLabels);
                CacheProvider.put(ES_CACHE, generateKey(resourceIDField, resourceName, lookupURL), tags);
            }
            return tags;
        }
                
        return Collections.emptyMap();
    }
	
    private String generateKey(String... fields){
        return String.join("#", fields);
    }
    public List<MetricFamilySamples> collect() {
        return collect(null);
    }
	public List<MetricFamilySamples> collect(String requestedMetricNamespace) {
      long start = System.nanoTime();
      double error = 0;
      List<MetricFamilySamples> mfs = new ArrayList<MetricFamilySamples>();
      try {
        scrape(requestedMetricNamespace, mfs);
      } catch (Exception e) {
        error = 1;
        LOGGER.log(Level.WARNING, "CloudWatch scrape failed", e);
      }
      List<MetricFamilySamples.Sample> samples = new ArrayList<MetricFamilySamples.Sample>();
      samples.add(new MetricFamilySamples.Sample(
          "cloudwatch_exporter_scrape_duration_seconds", new ArrayList<String>(), new ArrayList<String>(), (System.nanoTime() - start) / 1.0E9));
      mfs.add(new MetricFamilySamples("cloudwatch_exporter_scrape_duration_seconds", Type.GAUGE, "Time this CloudWatch scrape took, in seconds.", samples));

      samples = new ArrayList<MetricFamilySamples.Sample>();
      samples.add(new MetricFamilySamples.Sample(
          "cloudwatch_exporter_scrape_error", new ArrayList<String>(), new ArrayList<String>(), error));
      mfs.add(new MetricFamilySamples("cloudwatch_exporter_scrape_error", Type.GAUGE, "Non-zero if this scrape failed.", samples));
      
      samples = new ArrayList<MetricFamilySamples.Sample>();
      samples.add(new MetricFamilySamples.Sample(
          "cloudwatch_exporter_cache_usage", Arrays.asList("cache_name"), Arrays.asList(DIMENSIONS_CACHE), CacheProvider.getStatistics(DIMENSIONS_CACHE).getLocalHeapSize()));
      samples.add(new MetricFamilySamples.Sample(
          "cloudwatch_exporter_cache_usage", Arrays.asList("cache_name"), Arrays.asList(METRICS_CACHE), CacheProvider.getStatistics(METRICS_CACHE).getLocalHeapSize()));
      samples.add(new MetricFamilySamples.Sample(
          "cloudwatch_exporter_cache_usage", Arrays.asList("cache_name"), Arrays.asList(ES_CACHE), CacheProvider.getStatistics(ES_CACHE).getLocalHeapSize()));
      mfs.add(new MetricFamilySamples("cloudwatch_exporter_cache_usage", Type.GAUGE, "Memory used by cache.", samples));
      
      samples = new ArrayList<MetricFamilySamples.Sample>();
      samples.add(new MetricFamilySamples.Sample(
          "cloudwatch_exporter_cache_hitratio", Arrays.asList("cache_name"), Arrays.asList(DIMENSIONS_CACHE), CacheProvider.getStatistics(DIMENSIONS_CACHE).cacheHitRatio()));
      samples.add(new MetricFamilySamples.Sample(
          "cloudwatch_exporter_cache_hitratio", Arrays.asList("cache_name"), Arrays.asList(METRICS_CACHE), CacheProvider.getStatistics(METRICS_CACHE).cacheHitRatio()));
      samples.add(new MetricFamilySamples.Sample(
          "cloudwatch_exporter_cache_hitratio", Arrays.asList("cache_name"), Arrays.asList(ES_CACHE), CacheProvider.getStatistics(ES_CACHE).cacheHitRatio()));
      mfs.add(new MetricFamilySamples("cloudwatch_exporter_cache_hitratio", Type.GAUGE, "Cache Hit Ratio.", samples));
      
      samples = new ArrayList<MetricFamilySamples.Sample>();
      samples.add(new MetricFamilySamples.Sample(
          "cloudwatch_exporter_cache_hitcount", Arrays.asList("cache_name"), Arrays.asList(DIMENSIONS_CACHE), CacheProvider.getStatistics(DIMENSIONS_CACHE).cacheHitCount()));
      samples.add(new MetricFamilySamples.Sample(
          "cloudwatch_exporter_cache_hitcount", Arrays.asList("cache_name"), Arrays.asList(METRICS_CACHE), CacheProvider.getStatistics(METRICS_CACHE).cacheHitCount()));
      samples.add(new MetricFamilySamples.Sample(
          "cloudwatch_exporter_cache_hitcount", Arrays.asList("cache_name"), Arrays.asList(ES_CACHE), CacheProvider.getStatistics(ES_CACHE).cacheHitCount()));
      mfs.add(new MetricFamilySamples("cloudwatch_exporter_cache_hitcount", Type.COUNTER, "Cache Hit Count.", samples));
      
      samples = new ArrayList<MetricFamilySamples.Sample>();
      samples.add(new MetricFamilySamples.Sample(
          "cloudwatch_exporter_cache_misscount", Arrays.asList("cache_name"), Arrays.asList(DIMENSIONS_CACHE), CacheProvider.getStatistics(DIMENSIONS_CACHE).cacheMissCount()));
      samples.add(new MetricFamilySamples.Sample(
          "cloudwatch_exporter_cache_misscount", Arrays.asList("cache_name"), Arrays.asList(METRICS_CACHE), CacheProvider.getStatistics(METRICS_CACHE).cacheMissCount()));
      samples.add(new MetricFamilySamples.Sample(
          "cloudwatch_exporter_cache_misscount", Arrays.asList("cache_name"), Arrays.asList(ES_CACHE), CacheProvider.getStatistics(ES_CACHE).cacheMissCount()));
      mfs.add(new MetricFamilySamples("cloudwatch_exporter_cache_misscount", Type.COUNTER, "Cache Miss Count.", samples));
      return mfs;
    }
	
    static String readFile(String path) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded);
    }
    /**
     * Convenience function to run standalone.
     */
	public static void main(String[] args) throws Exception {
        
      String region = "us-west-2";
      if (args.length > 1) {
        region = args[1];
      }
      String yaml = "D:/Dev/newWorkspace/cloudwatch_exporter/example.yml";
      if (args.length > 0) {
          yaml = args[0];
      }
      yaml = readFile(yaml);
      CloudWatchCollector jc = new CloudWatchCollector(yaml);
//      ("{"
//      + "`region`: `" + region + "`,"
//      + "`metrics`: [{`aws_namespace`: `AWS/ELB`, `aws_metric_name`: `RequestCount`, `aws_dimensions`: [`AvailabilityZone`, `LoadBalancerName`]}] ,"
//      + "}").replace('`', '"'));
      for(MetricFamilySamples mfs : jc.collect()) {
        System.out.println(mfs);
      }
    }
}

