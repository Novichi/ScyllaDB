package org.example;

import java.io.File;
import java.io.IOException;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;

public class Main {
    public static void main(String[] args) throws IOException {
        File bundleFile = new File("D:/Uczelnia/ScyllaDB/untitled/connect-bundle-Informatyka_UE_2023.yaml");

        Cluster cluster = Cluster.builder()
                .withLoadBalancingPolicy(DCAwareRoundRobinPolicy.builder().withLocalDc("us-east-1").build())
                .withScyllaCloudConnectionConfig(bundleFile)
                .build();

        for (Host host: cluster.getMetadata().getAllHosts()) {
            System.out.printf("Datacenter: %s, Host: %s, Rack: %s\n",
                    host.getDatacenter(), host.getEndPoint(), host.getRack());
        }

        Session session = cluster.connect();

        System.out.println("Connected to cluster " + cluster.getMetadata().getClusterName());
        ResultSet resultSet = session.execute("SELECT * FROM system.clients LIMIT 10");

        for (Row row: resultSet.all()) {
            System.out.println(row.toString());
        }

    }
}