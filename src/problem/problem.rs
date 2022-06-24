use crate::{errors, image::strategy};
use multiprocessing::Object;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;

#[derive(Clone, Deserialize, Serialize)]
pub struct ProblemRevision {
    pub dependency_dag: DependencyDAG,
    pub strategy_factory: strategy::StrategyFactory,
    pub data: ProblemRevisionData,
}

#[derive(Object, Clone, Deserialize, Serialize)]
pub struct DependencyDAG {
    pub dependents_of: HashMap<u64, Vec<u64>>,
}

#[derive(Object, Clone)]
pub struct InstantiatedDependencyDAG {
    pub dag: DependencyDAG,
    pub disabled_tests: HashSet<u64>,
}

#[derive(Object, Clone, Deserialize, Serialize)]
pub struct ProblemRevisionData {
    pub problem_id: String,
    pub revision_id: String,
}

impl ProblemRevision {
    pub fn load_from_cache(path: &Path) -> Result<Self, errors::Error> {
        let config = std::fs::read(path.join("judging.msgpack")).map_err(|e| {
            errors::InvokerFailure(format!(
                "Could not read judging.msgpack to load problem from cache at {:?}: {:?}",
                path, e
            ))
        })?;

        let mut config: Self = rmp_serde::from_slice(&config).map_err(|e| {
            errors::ConfigurationFailure(format!(
                "Failed to parse judging.msgpack to load problem from cache at {:?}: {:?}",
                path, e
            ))
        })?;

        config.strategy_factory.root = path.to_owned();

        Ok(config)

        // Ok(Self {
        //     dependency_dag: DependencyDAG {
        //         dependents_of: HashMap::from([(1, vec![2]), (2, vec![3])]),
        //     },
        //     invocation_strategy_factory: Box::new(strategies::io::InputOutputStrategyFactory {
        //         checker: program::Program::load_from_cache(
        //             &format!("{}/bin/checker", path),
        //             image,
        //         )?,
        //     }),
        //     data: problem::ProblemRevisionData {
        //         problem_id,
        //         revision_id,
        //     },
        // })
    }
}

impl DependencyDAG {
    pub fn instantiate(self) -> InstantiatedDependencyDAG {
        InstantiatedDependencyDAG {
            dag: self,
            disabled_tests: HashSet::new(),
        }
    }
}

impl InstantiatedDependencyDAG {
    fn _fail_test(
        dependents_of: &HashMap<u64, Vec<u64>>,
        disabled_tests: &mut HashSet<u64>,
        test: u64,
    ) {
        if !disabled_tests.contains(&test) {
            disabled_tests.insert(test);
            for dep_test in dependents_of.get(&test).unwrap_or(&Vec::new()).iter() {
                Self::_fail_test(dependents_of, disabled_tests, *dep_test)
            }
        }
    }

    pub fn fail_test(&mut self, test: u64) {
        Self::_fail_test(&self.dag.dependents_of, &mut self.disabled_tests, test)
    }

    pub fn is_test_enabled(&self, test: u64) -> bool {
        !self.disabled_tests.contains(&test)
    }
}
