use std::{borrow::Borrow, cmp::Ordering};

use permutation::Permutation;

pub struct Data<Value> {
    pub(crate) data: Vec<Value>,
    sorting: Option<DataSorting<Value>>,
}

impl<Value> Default for Data<Value> {
    fn default() -> Self {
        Self {
            data: vec![],
            sorting: None,
        }
    }
}

impl<Value> Data<Value> {
    pub(crate) fn set(&mut self, new_data: impl Iterator<Item = Value>) {
        self.data.clear();
        self.data.extend(new_data);
    }

    pub fn sorted(&self) -> Vec<&Value> {
        let data_as_ref = self.data.iter().map(Borrow::borrow).collect();
        match self.sorting.as_ref() {
            Some(sorting) => sorting.permutation.apply_slice(data_as_ref),
            None => data_as_ref,
        }
    }

    pub(super) fn new_sorting(
        &mut self,
        sorting_fn: impl Fn(&Value, &Value) -> Ordering + Send + 'static,
    ) {
        let mut new_sorting = DataSorting::new(sorting_fn);
        new_sorting.resort(&self.data);
        let _ = self.sorting.insert(new_sorting);
    }

    fn resort(&mut self) {
        if let Some(soring) = self.sorting.as_mut() {
            soring.resort(&self.data)
        }
    }
}

struct DataSorting<Value> {
    permutation: Permutation,
    sorting_fn: SortingFn<Value>,
}

impl<Value> DataSorting<Value> {
    fn new(soring_fn: impl Fn(&Value, &Value) -> Ordering + Send + 'static) -> Self {
        Self {
            permutation: permutation::sort_by(Vec::<Value>::new(), &soring_fn),
            sorting_fn: Box::new(soring_fn),
        }
    }

    fn resort(&mut self, data: &Vec<Value>) {
        self.permutation = permutation::sort_by(data, |a, b| (self.sorting_fn)(a, b))
    }
}

type SortingFn<Value> = Box<dyn Fn(&Value, &Value) -> Ordering + Send + 'static>;
