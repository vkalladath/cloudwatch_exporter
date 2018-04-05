package io.prometheus.cloudwatch;

import java.util.Map;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;

public class CacheProvider {

    private static CacheProvider instance;
    CacheManager cm;
    Cache cache;

    private CacheProvider() {
        init();
    }

    public static CacheProvider getInstance() {
        if (instance == null) {
            instance = new CacheProvider();
        }
        return instance;
    }

    public void init() {
        cm = CacheManager.getInstance();
        CacheConfiguration cacheConfiguration = new CacheConfiguration().name("esCache").maxEntriesLocalHeap(100000).timeToLiveSeconds(2000);
        cm.addCache(new Cache(cacheConfiguration));

        cache = cm.getCache("esCache");
    }

    public Map<String, String> getFromCache(String resourceIDField, String resourceName, String lookupURL) {
        String key = generateKey(resourceIDField, resourceName, lookupURL);
        Element element = cache.get(key);
        if(element == null) {
            return null;
        }
        return (Map<String, String>) element.getObjectValue();
    }

    private String generateKey(String resourceIDField, String resourceName, String lookupURL) {
        String key = resourceIDField + resourceName + lookupURL;
        return key;
    }

    public void put(String resourceIDField, String resourceName, String lookupURL, Map<String, String> tags) {
        String key = generateKey(resourceIDField, resourceName, lookupURL);

        cache.put(new Element(key, tags));
    }
}
