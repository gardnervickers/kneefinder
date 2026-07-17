//! Adapter operation discovery and workload-mix resolution.

use std::{collections::BTreeMap, fmt};

use serde::{Deserialize, Serialize};

use crate::{
    config::{OperationSelection, WeightedOperation, WorkloadConfig},
    protocol::{ArgumentValue, OperationDescriptor, OperationKind},
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolvedOperation {
    pub name: String,
    pub kind: OperationKind,
    pub weight: f64,
    pub arguments: BTreeMap<String, ArgumentValue>,
}

/// Validates adapter discovery data and resolves the configured workload.
pub fn resolve_operation_mix(
    config: &WorkloadConfig,
    advertised: &[OperationDescriptor],
) -> Result<Vec<ResolvedOperation>, WorkloadError> {
    let mut descriptors = BTreeMap::new();
    for descriptor in advertised {
        if descriptor.name.is_empty() {
            return Err(WorkloadError::EmptyAdvertisedName);
        }
        validate_argument_descriptors(descriptor)?;
        if descriptors
            .insert(descriptor.name.as_str(), descriptor)
            .is_some()
        {
            return Err(WorkloadError::DuplicateAdvertisedOperation(
                descriptor.name.clone(),
            ));
        }
    }

    let resolved = match &config.operations {
        OperationSelection::AdapterDefaults => advertised
            .iter()
            .filter(|operation| operation.enabled_by_default)
            .map(|operation| {
                resolve_descriptor(operation, operation.default_weight, &BTreeMap::new())
            })
            .collect::<Result<Vec<_>, _>>()?,
        OperationSelection::All => advertised
            .iter()
            .map(|operation| {
                resolve_descriptor(operation, operation.default_weight, &BTreeMap::new())
            })
            .collect::<Result<Vec<_>, _>>()?,
        OperationSelection::Selected { operations } => resolve_selected(operations, &descriptors)?,
    };

    if resolved.is_empty() {
        return Err(WorkloadError::NoOperationsSelected);
    }
    Ok(resolved)
}

fn resolve_selected(
    selected: &[WeightedOperation],
    advertised: &BTreeMap<&str, &OperationDescriptor>,
) -> Result<Vec<ResolvedOperation>, WorkloadError> {
    let mut seen = BTreeMap::new();
    selected
        .iter()
        .map(|selection| {
            let descriptor = advertised
                .get(selection.name.as_str())
                .ok_or_else(|| WorkloadError::UnknownOperation(selection.name.clone()))?;
            let resolved = resolve_descriptor(descriptor, selection.weight, &selection.arguments)?;
            let variant = (resolved.name.clone(), resolved.arguments.clone());
            if seen.insert(variant, ()).is_some() {
                return Err(WorkloadError::DuplicateSelectedVariant {
                    operation: resolved.name,
                    arguments: resolved.arguments,
                });
            }
            Ok(resolved)
        })
        .collect()
}

fn resolve_descriptor(
    descriptor: &OperationDescriptor,
    weight: f64,
    configured_arguments: &BTreeMap<String, ArgumentValue>,
) -> Result<ResolvedOperation, WorkloadError> {
    if !weight.is_finite() || weight <= 0.0 {
        return Err(WorkloadError::InvalidWeight(descriptor.name.clone()));
    }
    Ok(ResolvedOperation {
        name: descriptor.name.clone(),
        kind: descriptor.kind,
        weight,
        arguments: resolve_arguments(descriptor, configured_arguments)?,
    })
}

fn validate_argument_descriptors(descriptor: &OperationDescriptor) -> Result<(), WorkloadError> {
    let mut seen = BTreeMap::new();
    for argument in &descriptor.arguments {
        if argument.name.is_empty() {
            return Err(WorkloadError::EmptyArgumentName(descriptor.name.clone()));
        }
        if seen.insert(&argument.name, ()).is_some() {
            return Err(WorkloadError::DuplicateAdvertisedArgument {
                operation: descriptor.name.clone(),
                argument: argument.name.clone(),
            });
        }
        if argument
            .default
            .as_ref()
            .is_some_and(|value| value.kind() != argument.kind)
        {
            return Err(WorkloadError::InvalidArgumentDefault {
                operation: descriptor.name.clone(),
                argument: argument.name.clone(),
            });
        }
    }
    Ok(())
}

fn resolve_arguments(
    descriptor: &OperationDescriptor,
    configured: &BTreeMap<String, ArgumentValue>,
) -> Result<BTreeMap<String, ArgumentValue>, WorkloadError> {
    for argument in configured.keys() {
        if !descriptor
            .arguments
            .iter()
            .any(|advertised| &advertised.name == argument)
        {
            return Err(WorkloadError::UnknownArgument {
                operation: descriptor.name.clone(),
                argument: argument.clone(),
            });
        }
    }

    descriptor
        .arguments
        .iter()
        .filter_map(|argument| {
            let value = configured
                .get(&argument.name)
                .cloned()
                .or_else(|| argument.default.clone());
            match value {
                Some(value) if value.kind() != argument.kind => {
                    Some(Err(WorkloadError::InvalidArgumentType {
                        operation: descriptor.name.clone(),
                        argument: argument.name.clone(),
                    }))
                }
                Some(value) => Some(Ok((argument.name.clone(), value))),
                None if argument.required => Some(Err(WorkloadError::MissingArgument {
                    operation: descriptor.name.clone(),
                    argument: argument.name.clone(),
                })),
                None => None,
            }
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkloadError {
    EmptyAdvertisedName,
    DuplicateAdvertisedOperation(String),
    DuplicateSelectedVariant {
        operation: String,
        arguments: BTreeMap<String, ArgumentValue>,
    },
    UnknownOperation(String),
    InvalidWeight(String),
    NoOperationsSelected,
    EmptyArgumentName(String),
    DuplicateAdvertisedArgument {
        operation: String,
        argument: String,
    },
    InvalidArgumentDefault {
        operation: String,
        argument: String,
    },
    UnknownArgument {
        operation: String,
        argument: String,
    },
    MissingArgument {
        operation: String,
        argument: String,
    },
    InvalidArgumentType {
        operation: String,
        argument: String,
    },
}

impl fmt::Display for WorkloadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyAdvertisedName => {
                formatter.write_str("adapter advertised an empty operation name")
            }
            Self::DuplicateAdvertisedOperation(name) => {
                write!(
                    formatter,
                    "adapter advertised operation {name:?} more than once"
                )
            }
            Self::DuplicateSelectedVariant {
                operation,
                arguments,
            } => write!(
                formatter,
                "operation variant {operation:?} with arguments {arguments:?} was selected more than once"
            ),
            Self::UnknownOperation(name) => {
                write!(formatter, "adapter does not advertise operation {name:?}")
            }
            Self::InvalidWeight(name) => {
                write!(formatter, "operation {name:?} has an invalid weight")
            }
            Self::NoOperationsSelected => formatter.write_str("no operations were selected"),
            Self::EmptyArgumentName(operation) => {
                write!(
                    formatter,
                    "operation {operation:?} advertises an empty argument name"
                )
            }
            Self::DuplicateAdvertisedArgument {
                operation,
                argument,
            } => write!(
                formatter,
                "operation {operation:?} advertises argument {argument:?} more than once"
            ),
            Self::InvalidArgumentDefault {
                operation,
                argument,
            } => write!(
                formatter,
                "operation {operation:?} has an invalid default for argument {argument:?}"
            ),
            Self::UnknownArgument {
                operation,
                argument,
            } => write!(
                formatter,
                "operation {operation:?} does not advertise argument {argument:?}"
            ),
            Self::MissingArgument {
                operation,
                argument,
            } => write!(
                formatter,
                "operation {operation:?} requires argument {argument:?}"
            ),
            Self::InvalidArgumentType {
                operation,
                argument,
            } => write!(
                formatter,
                "operation {operation:?} received the wrong type for argument {argument:?}"
            ),
        }
    }
}

impl std::error::Error for WorkloadError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{ArgumentKind, OperationArgument};

    fn operations() -> Vec<OperationDescriptor> {
        vec![
            OperationDescriptor {
                name: "read".into(),
                description: None,
                kind: OperationKind::Read,
                enabled_by_default: true,
                default_weight: 9.0,
                arguments: vec![OperationArgument {
                    name: "key".into(),
                    description: None,
                    kind: ArgumentKind::Integer,
                    required: true,
                    default: Some(ArgumentValue::Integer(0)),
                }],
            },
            OperationDescriptor {
                name: "write".into(),
                description: None,
                kind: OperationKind::Write,
                enabled_by_default: false,
                default_weight: 1.0,
                arguments: vec![OperationArgument {
                    name: "value".into(),
                    description: None,
                    kind: ArgumentKind::String,
                    required: true,
                    default: Some(ArgumentValue::String("demo".into())),
                }],
            },
        ]
    }

    fn workload(operations: OperationSelection) -> WorkloadConfig {
        WorkloadConfig { operations }
    }

    #[test]
    fn adapter_defaults_do_not_silently_include_opt_in_operations() {
        let resolved = resolve_operation_mix(
            &workload(OperationSelection::AdapterDefaults),
            &operations(),
        )
        .unwrap();

        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].name, "read");
    }

    #[test]
    fn all_is_an_explicit_way_to_include_every_operation() {
        let config = workload(OperationSelection::All);
        let resolved = resolve_operation_mix(&config, &operations()).unwrap();

        assert_eq!(
            resolved
                .iter()
                .map(|operation| operation.name.as_str())
                .collect::<Vec<_>>(),
            vec!["read", "write"]
        );
    }

    #[test]
    fn selected_mix_uses_user_weights_and_rejects_unknown_names() {
        let selection = OperationSelection::Selected {
            operations: vec![WeightedOperation {
                name: "write".into(),
                weight: 3.0,
                arguments: BTreeMap::from([("value".into(), ArgumentValue::String("demo".into()))]),
            }],
        };
        let config = workload(selection);
        let resolved = resolve_operation_mix(&config, &operations()).unwrap();
        assert_eq!(resolved[0].weight, 3.0);

        let unknown = OperationSelection::Selected {
            operations: vec![WeightedOperation {
                name: "delete".into(),
                weight: 1.0,
                arguments: BTreeMap::new(),
            }],
        };
        assert!(matches!(
            resolve_operation_mix(&workload(unknown), &operations()),
            Err(WorkloadError::UnknownOperation(name)) if name == "delete"
        ));
    }

    #[test]
    fn operation_arguments_apply_defaults_and_validate_types() {
        let config = workload(OperationSelection::AdapterDefaults);
        let resolved = resolve_operation_mix(&config, &operations()).unwrap();
        assert_eq!(
            resolved[0].arguments.get("key"),
            Some(&ArgumentValue::Integer(0))
        );

        let wrong_type = workload(OperationSelection::Selected {
            operations: vec![WeightedOperation {
                name: "read".into(),
                weight: 1.0,
                arguments: BTreeMap::from([("key".into(), ArgumentValue::String("zero".into()))]),
            }],
        });
        assert!(matches!(
            resolve_operation_mix(&wrong_type, &operations()),
            Err(WorkloadError::InvalidArgumentType { .. })
        ));
    }

    #[test]
    fn the_same_operation_with_different_arguments_forms_distinct_variants() {
        let config = workload(OperationSelection::Selected {
            operations: vec![
                WeightedOperation {
                    name: "read".into(),
                    weight: 3.0,
                    arguments: BTreeMap::from([("key".into(), ArgumentValue::Integer(0))]),
                },
                WeightedOperation {
                    name: "read".into(),
                    weight: 1.0,
                    arguments: BTreeMap::from([("key".into(), ArgumentValue::Integer(1))]),
                },
            ],
        });

        let resolved = resolve_operation_mix(&config, &operations()).unwrap();
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].weight, 3.0);
        assert_eq!(resolved[1].weight, 1.0);
        assert_ne!(resolved[0].arguments, resolved[1].arguments);
    }

    #[test]
    fn duplicate_variants_are_detected_after_applying_defaults() {
        let config = workload(OperationSelection::Selected {
            operations: vec![
                WeightedOperation {
                    name: "read".into(),
                    weight: 3.0,
                    arguments: BTreeMap::new(),
                },
                WeightedOperation {
                    name: "read".into(),
                    weight: 1.0,
                    arguments: BTreeMap::from([("key".into(), ArgumentValue::Integer(0))]),
                },
            ],
        });

        assert!(matches!(
            resolve_operation_mix(&config, &operations()),
            Err(WorkloadError::DuplicateSelectedVariant { operation, .. }) if operation == "read"
        ));
    }
}
