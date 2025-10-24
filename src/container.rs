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

pub fn create_name<C, T>() -> String {
    format!("{}<{}>", type_name::<C>(2), type_name::<T>(1))
}

fn type_name<T>(depth_from_back: usize) -> String {
    let mut name = std::any::type_name::<T>().to_owned();
    if let Some(pos) = name.find('<') {
        name = name.chars().take(pos).collect::<String>();
    }
    let mut parts = name.split("::").collect::<Vec<_>>();
    parts.reverse();
    parts
        .into_iter()
        .take(depth_from_back)
        .rev()
        .collect::<Vec<_>>()
        .join("::")
}

#[cfg(test)]
mod tests {
    use crate::container::{create_name, type_name};

    #[allow(dead_code)]
    struct SomeGeneric<One, Two> {
        one: One,
        two: Two,
    }

    #[test]
    fn crate_name_for_two_strings_returns_expected() {
        let name = create_name::<String, String>();
        assert_eq!("string::String<String>", name.as_str());
    }

    #[test]
    fn create_name_returns_correct_type_for_generics() {
        let name = create_name::<SomeGeneric<f64, i32>, String>();
        assert_eq!("tests::SomeGeneric<String>", name.as_str());
    }

    #[test]
    fn type_name_for_too_deep_depth_doesnt_error() {
        let name = type_name::<String>(100);
        assert_eq!(std::any::type_name::<String>(), name);
    }

    #[test]
    fn type_name_for_depth_0_returns_empty_string() {
        let name = type_name::<String>(0);
        assert_eq!("", name.as_str());
    }

    #[test]
    fn type_name_returns_correct_for_function_pointer() {
        let name = type_name::<SomeGeneric<fn(i32) -> String, f64>>(2);

        assert_eq!("tests::SomeGeneric", name.as_str());
    }
}
