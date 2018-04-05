package io.prometheus.cloudwatch;

public class Test {
    public static void main(String[] args) {
        CacheProvider c = CacheProvider.getInstance();
        c.getFromCache("a", "", "");
    }
}
