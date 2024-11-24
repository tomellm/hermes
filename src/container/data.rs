use std::{borrow::Borrow, cmp::Ordering};

use permutation::Permutation;

pub struct Data<Value> {
    pub(crate) data: Vec<Value>,
    sorting: Option<DataSorting<Value>>,
    has_changed: bool,
}

impl<Value> Default for Data<Value> {
    fn default() -> Self {
        Self {
            data: vec![],
            sorting: None,
            has_changed: true,
        }
    }
}

impl<Value> Data<Value> {
    pub(crate) fn set(&mut self, new_data: impl Iterator<Item = Value>) {
        self.data.clear();
        self.data.extend(new_data);
        self.has_changed = true;
        self.resort();
    }

    pub(crate) fn set_viewed(&mut self) {
        self.has_changed = false;
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

pub trait ImplData<Value> {
    fn data(&self) -> &Vec<Value>;
    fn sort(&mut self, sorting_fn: impl Fn(&Value, &Value) -> Ordering + Send + 'static);
    fn sorted(&self) -> Vec<&Value>;
    fn has_changed(&self) -> bool;
    fn set_viewed(&mut self) -> &mut Self;
}

impl<T, Value> ImplData<Value> for T
where
    T: HasData<Value>,
{
    fn data(&self) -> &Vec<Value> {
        &self.ref_data().data
    }
    fn sort(&mut self, sorting_fn: impl Fn(&Value, &Value) -> Ordering + Send + 'static) {
        self.ref_mut_data().new_sorting(sorting_fn);
    }
    fn sorted(&self) -> Vec<&Value> {
        self.ref_data().sorted()
    }
    fn has_changed(&self) -> bool {
        self.ref_data().has_changed
    }
    fn set_viewed(&mut self) -> &mut Self {
        self.ref_mut_data().set_viewed();
        self
    }
}

pub(crate) trait HasData<Value> {
    fn ref_data(&self) -> &Data<Value>;
    fn ref_mut_data(&mut self) -> &mut Data<Value>;
}
