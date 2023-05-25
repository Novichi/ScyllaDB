package org.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import java.util.UUID;

public class DatabaseOperations {
    DatabaseOperations(Session session){
        //Utworzenie Keyspace'a
        String createKeyspaceQuery = "CREATE KEYSPACE IF NOT EXISTS scylla_keyspace " +
                "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};";

        // Utwórz tabelę
        String createTableQuery = "CREATE TABLE IF NOT EXISTS scylla_keyspace.students (" +
                "id UUID PRIMARY KEY," +
                "name TEXT," +
                "age INT" +
                ");";

        // Wyświetl tabele students
        String selectAll = "SELECT * FROM scylla_keyspace.students";

        // Dodaj rekord do tabeli
        Insert insertQuery = QueryBuilder.insertInto("scylla_keyspace", "students")
                .value("id", UUID.randomUUID())
                .value("name", "John Doe")
                .value("age", 30);
        session.execute(createTableQuery);

        // Usuwanie rekordów z tabeli
        Delete deleteQuery = QueryBuilder.delete()
                .from("scylla_keyspace", "students")
                .where(QueryBuilder.eq("id", UUID.fromString("6a89ec6d-e036-4775-8a0e-530534444eab"))).ifExists();

        // Aktualizacja rekordu
        String updateQuery = "UPDATE scylla_keyspace.students SET Name = 'Lysy Chuj', Age = 40 WHERE id = e898475e-ab1a-4c5e-aad9-72a921b79ae8";

//        System.out.println(session.execute(createKeyspaceQuery));
//        System.out.println(session.execute(createTableQuery));
//        System.out.println(session.execute(insertQuery));

//        session.execute(deleteQuery);
        session.execute(updateQuery);

        ResultSet resultSet = session.execute(selectAll);
        for (Row row : resultSet) {
            // Pobieranie wartości kolumn z wiersza
            UUID id = row.getUUID("id");
            String name = row.getString("name");
            int age = row.getInt("age");

            // Przetwarzanie wartości...
            System.out.println("ID: " + id + ", Name: " + name + ", Age: " + age);
        }

        // Zamknij sesję i połączenie
        session.close();
    }
}
