-- CREATE TABLE northwind.books (
--   id varchar,
--   primary key(id)
-- );

CREATE TABLE IF NOT EXISTS books (
  id varchar,
  primary key(id)
);

/*
A tenant is a customer of pgEdge. A tenant may be automatically created for a
user upon signup. Tenants have a single owner, which is a user. Tenants may
have any number of users, as defined by the memberships table.
 */

CREATE TABLE IF NOT EXISTS booksToo (
  id varchar,
  primary key(id)
);
