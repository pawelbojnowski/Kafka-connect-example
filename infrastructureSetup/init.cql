create keyspace kafka_connect_example with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

use kafka_connect_example;

create table user
(
    id           int primary key,
    firstname    text,
    lastname     text,
    phone_number int
);

INSERT INTO user (id, firstname, lastname, phone_number)
VALUES (1, 'Jan', 'Nowak', 100100100);
INSERT INTO user (id, firstname, lastname, phone_number)
VALUES (2, 'Tomasz', 'Kowalski', 200200200);
INSERT INTO user (id, firstname, lastname, phone_number)
VALUES (3, 'Tomasz', 'Nowak', 300300300);

create table client
(
    client_id    UUID primary key,
    id           int,
    firstname    text,
    lastname     text,
    phone_number int
);
