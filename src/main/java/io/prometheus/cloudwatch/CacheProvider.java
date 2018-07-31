package io.prometheus.cloudwatch;

import java.util.HashMap;
import java.util.Map;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.statistics.StatisticsGateway;

public class CacheProvider {

    private static CacheManager cm = new CacheManager();

    public static Object getFromCache(String cacheName, String key) {
        Cache cache = cm.getCache(cacheName);
        if (cache == null) {
            cache = initCache(cacheName);
        }
        Element element = cache.get(key);
        if (element == null) {
            return null;
        }
        return element.getObjectValue();
    }

    public static Cache initCache(String cacheName, int maxEntries, long ttlSeconds) {
        CacheConfiguration cacheConfiguration = new CacheConfiguration().name(cacheName).maxEntriesLocalHeap(maxEntries).timeToLiveSeconds(ttlSeconds);
        cm.addCache(new Cache(cacheConfiguration));
        return cm.getCache(cacheName);
    }

    private static Cache initCache(String cacheName) {
        return initCache(cacheName, 100000, 2000);
    }

    public static void put(String cacheName, String key, Object element) {
        Cache cache = cm.getCache(cacheName);
        if (cache == null) {
            cache = initCache(cacheName);
        }
        if (element != null) {
            cache.put(new Element(key, element));
        }
    }
    
    public static StatisticsGateway getStatistics(String cacheName){
        Cache cache = cm.getCache(cacheName);
        return cache.getStatistics();
    }
}
