pub mod data;
pub mod tasked;

#[cfg(any(feature = "psql", feature = "mysql", feature = "sqlite"))]
pub mod builder;
#[cfg(any(feature = "psql", feature = "mysql", feature = "sqlite"))]
pub mod manual;
#[cfg(any(feature = "psql", feature = "mysql", feature = "sqlite"))]
pub mod projecting;
#[cfg(any(feature = "psql", feature = "mysql", feature = "sqlite"))]
pub mod simple;
