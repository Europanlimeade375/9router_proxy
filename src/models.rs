use serde::Deserialize;
use serde_json::{Map, Value};
use std::{collections::HashMap, error::Error, fmt, fs, path::Path, sync::Arc};

const SUPPORTED_VERSION: u32 = 1;

#[derive(Debug)]
pub(crate) struct ModelRegistry {
    aliases: HashMap<String, Arc<ModelProfile>>,
    profile_count: usize,
}

#[derive(Debug)]
struct ModelProfile {
    target: String,
    body: Map<String, Value>,
    remove: Vec<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelConfigFile {
    version: u32,
    #[serde(default)]
    models: Vec<ModelDefinition>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelDefinition {
    aliases: Vec<String>,
    target: String,
    #[serde(default)]
    remove: Vec<String>,
    #[serde(default)]
    body: Map<String, Value>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ModelConfigError(String);

impl ModelConfigError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for ModelConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for ModelConfigError {}

impl ModelRegistry {
    pub(crate) fn load(path: &Path) -> Result<Self, ModelConfigError> {
        let contents = fs::read_to_string(path).map_err(|error| {
            ModelConfigError::new(format!(
                "failed to read model configuration {}: {error}",
                path.display()
            ))
        })?;
        let extension = path
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or_default();
        match extension.to_ascii_lowercase().as_str() {
            "toml" => Self::parse_toml(&contents),
            "json" => Self::parse_json(&contents),
            _ => Err(ModelConfigError::new(
                "MODEL_CONFIG_PATH must end in .toml or .json",
            )),
        }
    }

    pub(crate) fn parse_toml(contents: &str) -> Result<Self, ModelConfigError> {
        let config = toml::from_str::<ModelConfigFile>(contents).map_err(|error| {
            ModelConfigError::new(format!("invalid TOML model config: {error}"))
        })?;
        Self::compile(config)
    }

    pub(crate) fn parse_json(contents: &str) -> Result<Self, ModelConfigError> {
        let config = serde_json::from_str::<ModelConfigFile>(contents).map_err(|error| {
            ModelConfigError::new(format!("invalid JSON model config: {error}"))
        })?;
        Self::compile(config)
    }

    fn compile(config: ModelConfigFile) -> Result<Self, ModelConfigError> {
        if config.version != SUPPORTED_VERSION {
            return Err(ModelConfigError::new(format!(
                "unsupported model config version {}; expected {SUPPORTED_VERSION}",
                config.version
            )));
        }

        let profile_count = config.models.len();
        let mut aliases = HashMap::new();
        for (profile_index, definition) in config.models.into_iter().enumerate() {
            let number = profile_index + 1;
            let target = definition.target.trim();
            if target.is_empty() {
                return Err(ModelConfigError::new(format!(
                    "model profile {number} has an empty target"
                )));
            }
            if definition.aliases.is_empty() {
                return Err(ModelConfigError::new(format!(
                    "model profile {number} must define at least one alias"
                )));
            }
            if definition.body.contains_key("model") {
                return Err(ModelConfigError::new(format!(
                    "model profile {number} must use target instead of body.model"
                )));
            }

            let remove = definition
                .remove
                .iter()
                .map(|pointer| parse_json_pointer(pointer, number))
                .collect::<Result<Vec<_>, _>>()?;
            if remove
                .iter()
                .any(|tokens| tokens.len() == 1 && tokens[0] == "model")
            {
                return Err(ModelConfigError::new(format!(
                    "model profile {number} cannot remove /model"
                )));
            }

            let profile = Arc::new(ModelProfile {
                target: target.to_owned(),
                body: definition.body,
                remove,
            });
            for alias in definition.aliases {
                let alias = alias.trim();
                if alias.is_empty() {
                    return Err(ModelConfigError::new(format!(
                        "model profile {number} contains an empty alias"
                    )));
                }
                if aliases.insert(alias.to_owned(), profile.clone()).is_some() {
                    return Err(ModelConfigError::new(format!(
                        "duplicate model alias: {alias}"
                    )));
                }
            }
        }

        Ok(Self {
            aliases,
            profile_count,
        })
    }

    pub(crate) fn apply(&self, request_body: &mut Value) -> bool {
        let Some(alias) = request_body
            .as_object()
            .and_then(|object| object.get("model"))
            .and_then(Value::as_str)
        else {
            return false;
        };
        let Some(profile) = self.aliases.get(alias) else {
            return false;
        };

        for pointer in &profile.remove {
            remove_json_pointer(request_body, pointer);
        }
        merge_json(request_body, &Value::Object(profile.body.clone()));
        if let Some(object) = request_body.as_object_mut() {
            object.insert("model".to_owned(), Value::String(profile.target.clone()));
        }
        true
    }

    pub(crate) fn alias_count(&self) -> usize {
        self.aliases.len()
    }

    pub(crate) fn profile_count(&self) -> usize {
        self.profile_count
    }
}

fn parse_json_pointer(pointer: &str, profile: usize) -> Result<Vec<String>, ModelConfigError> {
    if pointer.is_empty() || !pointer.starts_with('/') {
        return Err(ModelConfigError::new(format!(
            "model profile {profile} has invalid remove pointer {pointer:?}"
        )));
    }
    pointer[1..]
        .split('/')
        .map(|token| decode_pointer_token(token, profile, pointer))
        .collect()
}

fn decode_pointer_token(
    token: &str,
    profile: usize,
    pointer: &str,
) -> Result<String, ModelConfigError> {
    let mut decoded = String::with_capacity(token.len());
    let mut characters = token.chars();
    while let Some(character) = characters.next() {
        if character != '~' {
            decoded.push(character);
            continue;
        }
        match characters.next() {
            Some('0') => decoded.push('~'),
            Some('1') => decoded.push('/'),
            _ => {
                return Err(ModelConfigError::new(format!(
                    "model profile {profile} has invalid remove pointer {pointer:?}"
                )));
            }
        }
    }
    Ok(decoded)
}

fn merge_json(target: &mut Value, patch: &Value) {
    match (target, patch) {
        (Value::Object(target), Value::Object(patch)) => {
            for (key, value) in patch {
                merge_json(target.entry(key.clone()).or_insert(Value::Null), value);
            }
        }
        (target, patch) => *target = patch.clone(),
    }
}

fn remove_json_pointer(root: &mut Value, tokens: &[String]) {
    let Some((last, parents)) = tokens.split_last() else {
        return;
    };
    let mut current = root;
    for token in parents {
        current = match current {
            Value::Object(object) => match object.get_mut(token) {
                Some(value) => value,
                None => return,
            },
            Value::Array(array) => match token
                .parse::<usize>()
                .ok()
                .and_then(|index| array.get_mut(index))
            {
                Some(value) => value,
                None => return,
            },
            _ => return,
        };
    }

    match current {
        Value::Object(object) => {
            object.remove(last);
        }
        Value::Array(array) => {
            if let Ok(index) = last.parse::<usize>()
                && index < array.len()
            {
                array.remove(index);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const NESTED_CONFIG: &str = r#"
version = 1

[[models]]
aliases = ["quality/example", "example(high)"]
target = "provider/example"
remove = ["/reasoning_effort", "/provider/old_mode", "/items/0/private"]

[models.body]
reasoning_effort = "high"

[models.body.thinking]
type = "enabled"
budget_tokens = 32000

[models.body.provider]
quality = "maximum"
"#;

    #[test]
    fn applies_recursive_overrides_and_nested_removals() {
        let registry = ModelRegistry::parse_toml(NESTED_CONFIG).expect("config should parse");
        let mut body = json!({
            "model": "quality/example",
            "reasoning_effort": "low",
            "thinking": {"type": "disabled", "preserved": true},
            "provider": {"old_mode": true, "preserved": 7},
            "items": [{"private": true, "kept": true}],
            "messages": []
        });

        assert!(registry.apply(&mut body));
        assert_eq!(body["model"], "provider/example");
        assert_eq!(body["reasoning_effort"], "high");
        assert_eq!(body["thinking"]["type"], "enabled");
        assert_eq!(body["thinking"]["budget_tokens"], 32000);
        assert_eq!(body["thinking"]["preserved"], true);
        assert_eq!(body["provider"]["quality"], "maximum");
        assert_eq!(body["provider"]["preserved"], 7);
        assert!(body["provider"].get("old_mode").is_none());
        assert!(body["items"][0].get("private").is_none());
        assert_eq!(body["items"][0]["kept"], true);
        assert_eq!(body["messages"], json!([]));
    }

    #[test]
    fn parses_the_same_schema_from_json() {
        let registry = ModelRegistry::parse_json(
            r#"{
                "version": 1,
                "models": [{
                    "aliases": ["quality/json"],
                    "target": "provider/json",
                    "body": {"reasoning": {"effort": "high"}}
                }]
            }"#,
        )
        .expect("JSON config should parse");
        let mut body = json!({"model": "quality/json"});

        assert!(registry.apply(&mut body));
        assert_eq!(body["model"], "provider/json");
        assert_eq!(body["reasoning"]["effort"], "high");
    }

    #[test]
    fn leaves_unknown_models_unchanged() {
        let registry = ModelRegistry::parse_toml(NESTED_CONFIG).expect("config should parse");
        let mut body = json!({"model": "provider/other", "temperature": 0.2});
        let original = body.clone();

        assert!(!registry.apply(&mut body));
        assert_eq!(body, original);
    }

    #[test]
    fn rejects_duplicate_aliases_and_invalid_pointers() {
        let duplicate = NESTED_CONFIG.replace(
            "aliases = [\"quality/example\", \"example(high)\"]",
            "aliases = [\"quality/example\", \"quality/example\"]",
        );
        assert_eq!(
            ModelRegistry::parse_toml(&duplicate)
                .expect_err("duplicate aliases should fail")
                .to_string(),
            "duplicate model alias: quality/example"
        );

        let invalid = NESTED_CONFIG.replace("/provider/old_mode", "provider/old_mode");
        assert!(
            ModelRegistry::parse_toml(&invalid)
                .expect_err("invalid pointer should fail")
                .to_string()
                .contains("invalid remove pointer")
        );
    }

    #[test]
    fn rejects_ambiguous_model_mutations() {
        let body_model = r#"
version = 1
[[models]]
aliases = ["alias"]
target = "target"
[models.body]
model = "other"
"#;
        assert!(
            ModelRegistry::parse_toml(body_model)
                .expect_err("body.model should fail")
                .to_string()
                .contains("must use target")
        );

        let remove_model = r#"
version = 1
[[models]]
aliases = ["alias"]
target = "target"
remove = ["/model"]
"#;
        assert!(
            ModelRegistry::parse_toml(remove_model)
                .expect_err("removing model should fail")
                .to_string()
                .contains("cannot remove /model")
        );
    }
}
