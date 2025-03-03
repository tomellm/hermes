# Hermes

Hermes helps you manage database dependent state in direct render systems.

With the power of the query builder provided by `sea_orm`, `hermes` allows you 
to connect to any database, execute queries on it and store the resulting data
in one of the many container implementations. Additionaly you can execute `update`
and `delete` queries on the database and `hermes` will automatically notify all
possibly affected containers of a possible change leading them to nofiy you or
automatically trigger a requery.

### ⚠️  Attention

`hermes` is tied to both `sea_orm` and `tokio` as of now!

```rust
// run queries that will update internal state on completion
records.query(Record::find().select());

// ---

// execute a query and notify all affected containers
records.execute(Record::insert_many(records));
```

Hermes does 3 main things for you:

1. It stores the results of queries for you
2. It notifies you, and (if requested) requeries, when a query is executed on
tables your data is related to
3. Provides useful utlities like projecting, efficient sorting, trasaction 
queries, change detection


## Why I created it

I specifically created this to be used in direct render systems (in my case `egui`)
where I often found myself storing the output of SQL queries in `Vec`'s and then
having to create some kind of refresh button to update the data manually since
the program had no way to know if the underlying data had changed.

The general concept of `hermes` is the following: There is a central unique `Messenger`
that need to be created first with some kind of database connection. All data
containers are then created from this `Messenger`. Each container can then
query for some data which will tell the `Messenger` what tables the container is
interested in. Later on when a container executes a `dml` query on the database
the `Messenger` will notify all affected containers that their data might have 
changed.

This System very simply works by looking at the querystring of the queries meaning
there can be a lot of missfireings because a `dml` query only had a table in the 
where clause and didn't actually change it. Nevertheless `hermes` allows me to 
have a place to put data, make use of some utilities, know when the data migh 
have changed and subsequently easily update and not have to deal with the asycorinicity
of executing the queries.

