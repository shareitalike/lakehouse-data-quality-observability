import os
import yaml
from typing import List, Dict, Any

from config.rule_configs import (
    BaseRuleConfig,
    NotNullRule,
    UniqueKeyRule,
    AcceptedValuesRule,
    PositiveNumericRule,
    TimestampFreshnessRule,
    DuplicateDetectionRule,
    SchemaDriftRule,
    ReferentialIntegrityRule,
    RowCountAnomalyRule,
    DistributionAnomalyRule
)

# Registry mapping string names from YAML to the actual dataclass types
RULE_TYPE_REGISTRY = {
    "NotNullRule": NotNullRule,
    "UniqueKeyRule": UniqueKeyRule,
    "AcceptedValuesRule": AcceptedValuesRule,
    "PositiveNumericRule": PositiveNumericRule,
    "TimestampFreshnessRule": TimestampFreshnessRule,
    "DuplicateDetectionRule": DuplicateDetectionRule,
    "SchemaDriftRule": SchemaDriftRule,
    "ReferentialIntegrityRule": ReferentialIntegrityRule,
    "RowCountAnomalyRule": RowCountAnomalyRule,
    "DistributionAnomalyRule": DistributionAnomalyRule,
}

def load_rules_from_yaml(file_path: str) -> List[BaseRuleConfig]:
    """
    Load rule configurations from a YAML file.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Rule configuration file not found: {file_path}")
        
    with open(file_path, 'r') as f:
        data = yaml.safe_load(f)
        
    if not data or 'rules' not in data:
        return []
        
    parsed_rules = []
    for rule_data in data['rules']:
        # Extract the rule_type which tells us which dataclass to instantiate
        rule_type_name = rule_data.pop('rule_type', None)
        if not rule_type_name:
            raise ValueError(f"Missing 'rule_type' in rule configuration: {rule_data.get('rule_id', 'Unknown')}")
            
        rule_class = RULE_TYPE_REGISTRY.get(rule_type_name)
        if not rule_class:
            raise ValueError(f"Unknown rule_type '{rule_type_name}' in configuration.")
            
        # Instantiate the correct dataclass with the remaining yaml attributes
        rule_instance = rule_class(**rule_data)
        parsed_rules.append(rule_instance)
        
    return parsed_rules
