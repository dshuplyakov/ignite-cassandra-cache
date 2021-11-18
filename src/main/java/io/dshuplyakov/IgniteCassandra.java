package io.dshuplyakov;

import com.datastax.driver.core.policies.RoundRobinPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.store.cassandra.CassandraCacheStoreFactory;
import org.apache.ignite.cache.store.cassandra.datasource.DataSource;
import org.apache.ignite.cache.store.cassandra.persistence.KeyValuePersistenceSettings;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.io.File;

public class IgniteCassandra {

    public static void main(String[] args) {

        IgniteConfiguration cfg = new IgniteConfiguration();
        CacheConfiguration configuration = new CacheConfiguration();

        //  Setting cache name
        configuration.setName("ignite-cassandra-test");

        //  Configuring Cassandra's persistence
        DataSource dataSource = new DataSource();
        dataSource.setContactPoints("127.0.0.1");
        RoundRobinPolicy robinPolicy = new RoundRobinPolicy();
        dataSource.setLoadBalancingPolicy(robinPolicy);
        dataSource.setReadConsistency("ONE");
        dataSource.setWriteConsistency("ONE");

        KeyValuePersistenceSettings persistenceSettings = new KeyValuePersistenceSettings(new File("primitive_persistence.xml"));
        CassandraCacheStoreFactory cacheStoreFactory = new CassandraCacheStoreFactory();
        cacheStoreFactory.setDataSource(dataSource);
        cacheStoreFactory.setPersistenceSettings(persistenceSettings);
        configuration.setCacheStoreFactory(cacheStoreFactory);
        configuration.setWriteThrough(true);
        configuration.setWriteBehindEnabled(true);
        configuration.setReadThrough(true);

        //  Sets the cache configuration
        cfg.setCacheConfiguration(configuration);

        //  Starting Ignite
        Ignite ignite = Ignition.start(cfg);

        IgniteCache cache = ignite.getOrCreateCache(configuration);

        for (int i = 0; i < 100; i++){
            cache.put(String.valueOf(i), "100");
        }
    }
}
