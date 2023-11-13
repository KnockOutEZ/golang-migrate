-- select spock.replicate_ddl('CREATE TABLE northwind.books (
--   id varchar,
--   primary key(id)
-- );');

CREATE TABLE northwind.books (
  id varchar,
  primary key(id)
);

-- select spock.replicate_ddl('create table books(id varchar, primary key(id));');