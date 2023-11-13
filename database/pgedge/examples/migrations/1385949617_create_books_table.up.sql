-- CREATE TABLE books (
--   user_id integer,
--   name    varchar(40),
--   author  varchar(40)
-- );

select spock.replicate_ddl('create table northwind.books(id varchar, primary key(id));');