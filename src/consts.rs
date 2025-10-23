use sea_orm::DbBackend;
#[cfg(feature = "mysql")]
use sea_query::MysqlQueryBuilder;
#[cfg(feature = "psql")]
use sea_query::PostgresQueryBuilder;
#[cfg(feature = "sqlite")]
use sea_query::SqliteQueryBuilder;

#[cfg(all(feature = "psql", not(feature = "mysql"), not(feature = "sqlite")))]
pub(crate) const DB_BACKEND: DbBackend = DbBackend::Postgres;

#[cfg(all(feature = "mysql", not(feature = "psql"), not(feature = "sqlite")))]
pub(crate) const DB_BACKEND: DbBackend = DbBackend::MySql;

#[cfg(all(feature = "sqlite", not(feature = "mysql"), not(feature = "psql")))]
pub(crate) const DB_BACKEND: DbBackend = DbBackend::Sqlite;

#[cfg(any(
    any(
        all(feature = "psql", feature = "mysql"),
        all(feature = "psql", feature = "sqlite"),
        all(feature = "mysql", feature = "sqlite")
    ),
    not(any(feature = "psql", feature = "mysql", feature = "sqlite"))
))]
pub(crate) const DB_BACKEND: DbBackend = panic!("you have to activate one of the features");

#[cfg(all(feature = "psql", not(feature = "mysql"), not(feature = "sqlite")))]
pub(crate) const QUERY_BUILDER: PostgresQueryBuilder = PostgresQueryBuilder;

#[cfg(all(feature = "mysql", not(feature = "psql"), not(feature = "sqlite")))]
pub(crate) const QUERY_BUILDER: MysqlQueryBuilder = MysqlQueryBuilder;

#[cfg(all(feature = "sqlite", not(feature = "mysql"), not(feature = "psql")))]
pub(crate) const QUERY_BUILDER: SqliteQueryBuilder = SqliteQueryBuilder;

#[cfg(any(
    any(
        all(feature = "psql", feature = "mysql"),
        all(feature = "psql", feature = "sqlite"),
        all(feature = "mysql", feature = "sqlite")
    ),
    not(any(feature = "psql", feature = "mysql", feature = "sqlite"))
))]
pub(crate) const QUERY_BUILDER: SqliteQueryBuilder =
    panic!("you have to activate one of the features");
