create schema if not exists public;
-- use public;

create table public.user
(
    id           int primary key,
    firstname    text,
    lastname     text,
    phone_number int
);

INSERT INTO public.user(id, firstname, lastname, phone_number)
VALUES (1, 'John', 'Wick', 100100100),
       (2, 'Jack', 'Sparrow', 200200200),
       (3, 'Brian', 'O’Conner', 300300300);

--
create table public.client
(
    id           int primary key,
    clientId           int primary key,
    firstname    text,
    lastname     text,
    phone_number int
)