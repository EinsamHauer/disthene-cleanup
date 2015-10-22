package net.iponweb.disthene.cleanup;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.base.Stopwatch;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author Andrei Ivanov
 */
public class Main {
    private static TransportClient client;
    private static final ExecutorService pool = Executors.newFixedThreadPool(8);
    private static PathNode root;

    public static void main(String[] args) throws InterruptedException {
        System.out.println(new DateTime());


        root = new PathNode();

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "cyanite")
                .build();

        client = new TransportClient(settings);
//        client.addTransportAddress(new InetSocketTransportAddress("es-1a.graphite.devops.iponweb.net", 9300));
        client.addTransportAddress(new InetSocketTransportAddress("elasticsearch1b.aws-va.graphite.iponweb.net", 9300));

        Stopwatch timer = Stopwatch.createStarted();

        final List<String> paths = new ArrayList<>();

        SearchResponse response = client.prepareSearch("cyanite_paths")
                .setScroll(new TimeValue(120000))
                .setSize(100000)
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.filteredQuery(
                        QueryBuilders.regexpQuery("path", ".*"),
                        FilterBuilders.termFilter("tenant", "cpa-exchange")), FilterBuilders.termFilter("leaf", true)))
/*
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.regexpQuery("path", "userverlua-aws-eu1\\..*"),
                        FilterBuilders.termFilter("tenant", "tokyo")))
*/
                .addField("path")
                .addField("leaf")
                .execute().actionGet();

        while (response.getHits().getHits().length > 0) {
            for (SearchHit hit : response.getHits()) {
                addNode(hit.field("path").<String>getValue(), hit.field("leaf").<Boolean>getValue());
            }

            response = client.prepareSearchScroll(response.getScrollId())
                    .setScroll(new TimeValue(120000))
                    .execute().actionGet();
        }

//        buildTree(root, paths);
//        pool.awaitTermination(30, TimeUnit.MINUTES);
        timer.stop();

//        System.out.println(root);

        printNode(root);

        System.out.println("Size: " + root.size());
        System.out.println("Elapsed: " + timer.elapsed(TimeUnit.SECONDS) + " seconds");
    }

    private static void addNode(String path, boolean leaf) {
        PathNode current = root;
        String currentPath = null;
        int index = path.indexOf(".");
        while (index > 0) {
            currentPath = path.substring(0, index);

            PathNode childNode = current.getChildren().get(currentPath);
            if (childNode == null) {
                childNode = new PathNode(currentPath, false);
                current.getChildren().put(currentPath, childNode);
            }

            current = childNode;
            index = path.indexOf(".", index + 1);
        }

        if (current.getChildren().get(path) == null) {
            current.getChildren().put(path, new PathNode(path, leaf));
        }

    }

    private static void printNode(PathNode node) {
        System.out.println(node.getPath());
        for (PathNode child : node.getChildren().values()) {
            printNode(child);
        }
    }

/*
    private static void buildTree(PathNode parent) {
        String regEx;

        if (parent.getPath() == null) {
            regEx = "[^\\.]*";
        } else {
            regEx = parent.getPath() + "\\.[^\\.]*";
        }

        SearchResponse response = client.prepareSearch("cyanite_paths")
                .setScroll(new TimeValue(12000))
                .setSize(10000)
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.regexpQuery("path", regEx),
                        FilterBuilders.termFilter("tenant", "tokyo")))
                .addField("path")
                .addField("leaf")
                .execute().actionGet();

        while (response.getHits().getHits().length > 0) {
            for (SearchHit hit : response.getHits()) {
                PathNode node = new PathNode();
                node.setPath((String) hit.field("path").getValue());
                node.setLeaf((boolean) hit.field("leaf").getValue());

                parent.getChildren().put(node.getPath(), node);
            }
            response = client.prepareSearchScroll(response.getScrollId())
                    .setScroll(new TimeValue(12000))
                    .execute().actionGet();
        }

        // recurse
        List<Future> futures = new ArrayList<>();
        for (final PathNode node : parent.getChildren().values()) {
            if (!node.isLeaf()) {
*/
/*
                if (parent.getPath() != null) {
                    buildTree(node);
                } else {
                    pool.submit(new Runnable() {
                        @Override
                        public void run() {
                            buildTree(node);
                        }
                    });
                }
*//*

                pool.submit(new Runnable() {
                    @Override
                    public void run() {
                        buildTree(node);
                    }
                });

            }
        }

    }
*/
}
