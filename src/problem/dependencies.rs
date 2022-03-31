use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Clone)]
pub struct DependencyDAG {
    pub dependents_of: Arc<HashMap<u64, Vec<u64>>>,
    pub disabled_tests: HashSet<u64>,
}

impl DependencyDAG {
    fn _fail_test(
        dependents_of: &HashMap<u64, Vec<u64>>,
        disabled_tests: &mut HashSet<u64>,
        test: u64,
    ) {
        if !disabled_tests.contains(&test) {
            disabled_tests.insert(test);
            for dep_test in dependents_of.get(&test).unwrap_or(&Vec::new()).iter() {
                DependencyDAG::_fail_test(dependents_of, disabled_tests, *dep_test)
            }
        }
    }

    pub fn fail_test(&mut self, test: u64) {
        DependencyDAG::_fail_test(&self.dependents_of, &mut self.disabled_tests, test)
    }

    pub fn is_test_enabled(&self, test: u64) -> bool {
        !self.disabled_tests.contains(&test)
    }
}
