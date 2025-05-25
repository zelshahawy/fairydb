// Reference: https://github.com/rotaki/decorrelator

use std::{
    collections::HashSet,
    sync::{Arc, RwLock},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Rule {
    Hoist,
    Decorrelate,
    SelectionPushdown,
    ProjectionPushdown,
}

pub struct Rules {
    rules: RwLock<HashSet<Rule>>,
}

impl Rules {
    pub fn new() -> Rules {
        Rules {
            rules: RwLock::new(HashSet::new()),
        }
    }

    pub fn enable(&self, rule: Rule) {
        self.rules.write().unwrap().insert(rule);
    }

    pub fn disable(&self, rule: Rule) {
        self.rules.write().unwrap().remove(&rule);
    }

    pub fn is_enabled(&self, rule: &Rule) -> bool {
        self.rules.read().unwrap().contains(rule)
    }
}

impl Default for Rules {
    fn default() -> Self {
        let mut rules = HashSet::new();
        rules.insert(Rule::Hoist);
        rules.insert(Rule::Decorrelate);
        rules.insert(Rule::SelectionPushdown);
        rules.insert(Rule::ProjectionPushdown);
        Rules {
            rules: RwLock::new(rules),
        }
    }
}

pub type RulesRef = Arc<Rules>;
