use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    fs,
    path::{Component, Path, PathBuf},
};

use anyhow::Context;
use dprint_plugin_typescript::{FormatTextOptions, configuration::ConfigurationBuilder};

use crate::{
    codegen_types::TypegenModuleRegistration,
    typegen_module::{Declaration, Operand, Predicate, Type, TypegenModule, Value},
};

fn string(value: &str) -> anyhow::Result<String> {
    // JSON strings are valid JavaScript strings and correctly handle quotes,
    // backslashes, control characters, and non-ASCII text.
    Ok(serde_json::to_string(value)?)
}

fn identifier(value: &str) -> anyhow::Result<&str> {
    let mut chars = value.chars();
    let valid = chars
        .next()
        .is_some_and(|c| c == '_' || c == '$' || c.is_ascii_alphabetic())
        && chars.all(|c| c == '_' || c == '$' || c.is_ascii_alphanumeric());
    anyhow::ensure!(valid, "invalid typegen identifier: {value:?}");
    Ok(value)
}

fn property(value: &str) -> anyhow::Result<String> {
    identifier(value).map_or_else(|_| string(value), |value| Ok(value.to_owned()))
}

fn render_type(ty: &Type) -> anyhow::Result<String> {
    Ok(match ty {
        Type::String => "string".into(),
        Type::Boolean => "boolean".into(),
        Type::Number => "number".into(),
        Type::Named(name) => identifier(name)?.into(),
        Type::Array(item) => format!("{}[]", render_type(item)?),
        Type::Optional(inner) => format!("{} | undefined", render_type(inner)?),
        Type::StringUnion(values) => values
            .iter()
            .map(|value| string(value))
            .collect::<anyhow::Result<Vec<_>>>()?
            .join(" | "),
        Type::Object(fields) => {
            let fields = fields
                .iter()
                .map(|f| {
                    Ok(format!(
                        "  {}{}: {};",
                        property(&f.name)?,
                        if f.optional { "?" } else { "" },
                        render_type(&f.ty)?
                    ))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            format!("{{\n{}\n}}", fields.join("\n"))
        }
        Type::Record(key, value) => {
            format!("Record<{}, {}>", render_type(key)?, render_type(value)?)
        }
    })
}

fn render_value(value: &Value, indent: usize) -> anyhow::Result<String> {
    Ok(match value {
        Value::String(v) => string(v)?,
        Value::Bool(v) => v.to_string(),
        Value::Integer(v) => v.to_string(),
        Value::Float(v) => {
            anyhow::ensure!(
                v.is_finite(),
                "typegen constants cannot contain non-finite floats"
            );
            v.to_string()
        }
        Value::Reference(v) => identifier(v)?.into(),
        Value::Array(values) => {
            if values.is_empty() {
                "[]".into()
            } else {
                let child_indent = indent
                    .checked_add(2)
                    .context("typegen value indentation overflow")?;
                let pad = " ".repeat(child_indent);
                let close = " ".repeat(indent);
                format!(
                    "[\n{}{},\n{}]",
                    pad,
                    values
                        .iter()
                        .map(|v| render_value(v, child_indent))
                        .collect::<anyhow::Result<Vec<_>>>()?
                        .join(&format!(",\n{pad}")),
                    close
                )
            }
        }
        Value::Object(entries) => {
            if entries.is_empty() {
                "{}".into()
            } else {
                let child_indent = indent
                    .checked_add(2)
                    .context("typegen value indentation overflow")?;
                let pad = " ".repeat(child_indent);
                let close = " ".repeat(indent);
                let rendered = entries
                    .iter()
                    .map(|(k, v)| {
                        Ok(format!(
                            "{}: {}",
                            property(k)?,
                            render_value(v, child_indent)?
                        ))
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                format!(
                    "{{\n{}{},\n{}}}",
                    pad,
                    rendered.join(&format!(",\n{pad}")),
                    close
                )
            }
        }
    })
}

fn render_operand(value: &Operand, parameter: &str) -> anyhow::Result<String> {
    Ok(match value {
        Operand::ParameterField(field) => format!("{parameter}.{}", identifier(field)?),
        Operand::String(value) => string(value)?,
        Operand::Bool(value) => value.to_string(),
    })
}

fn render_predicate(predicate: &Predicate, parameter: &str) -> anyhow::Result<String> {
    let (items, operator) = match predicate {
        Predicate::Equal(a, b) => {
            return Ok(format!(
                "{} === {}",
                render_operand(a, parameter)?,
                render_operand(b, parameter)?
            ));
        }
        Predicate::NotEqual(a, b) => {
            return Ok(format!(
                "{} !== {}",
                render_operand(a, parameter)?,
                render_operand(b, parameter)?
            ));
        }
        Predicate::And(items) => (items, " && "),
        Predicate::Or(items) => (items, " || "),
    };
    anyhow::ensure!(
        !items.is_empty(),
        "compound typegen predicate cannot be empty"
    );
    Ok(items
        .iter()
        .map(|p| render_predicate(p, parameter).map(|p| format!("({p})")))
        .collect::<anyhow::Result<Vec<_>>>()?
        .join(operator))
}

fn render_declaration(out: &mut String, declaration: &Declaration) -> anyhow::Result<()> {
    match declaration {
        Declaration::TypeAlias { name, doc, ty } => {
            if let Some(doc) = doc {
                writeln!(out, "/** {} */", doc.replace("*/", "* /"))?;
            }
            writeln!(out, "export type {name} = {}\n", render_type(ty)?)?;
        }
        Declaration::Const {
            name,
            doc,
            ty,
            value,
            immutable,
            satisfies,
        } => {
            if let Some(doc) = doc {
                writeln!(out, "/** {} */", doc.replace("*/", "* /"))?;
            }
            write!(out, "export const {name}")?;
            if let Some(ty) = ty {
                write!(out, ": {}", render_type(ty)?)?;
            }
            write!(out, " = {}", render_value(value, 0)?)?;
            if *immutable {
                out.push_str(" as const");
            }
            if let Some(ty) = satisfies {
                write!(out, " satisfies {}", render_type(ty)?)?;
            }
            out.push_str("\n\n");
        }
        Declaration::FilteredArray {
            name,
            source,
            parameter,
            predicate,
        } => {
            identifier(source)?;
            identifier(parameter)?;
            writeln!(
                out,
                "export const {name} = {source}.filter(({parameter}) => {})\n",
                render_predicate(predicate, parameter)?
            )?;
        }
        Declaration::Index {
            name,
            source,
            key_field,
            value_type,
        } => {
            identifier(source)?;
            identifier(key_field)?;
            writeln!(
                out,
                "export const {name}: Record<string, {}> = Object.fromEntries(\n  {source}.map((entry) => [entry.{key_field}, entry]),\n)\n",
                render_type(value_type)?
            )?;
        }
        Declaration::Find {
            name,
            index,
            parameter,
            key_type,
            value_type,
        } => {
            identifier(index)?;
            identifier(parameter)?;
            writeln!(
                out,
                "export function {name}({parameter}: {}): {} | undefined {{\n  return {index}[{parameter}]\n}}\n",
                render_type(key_type)?,
                render_type(value_type)?
            )?;
        }
        Declaration::LookupOr {
            name,
            index,
            parameter,
            key_type,
            value_type,
            fallback,
        } => {
            identifier(index)?;
            identifier(parameter)?;
            writeln!(
                out,
                "export function {name}({parameter}?: {}): {} {{\n  if (!{parameter}) return {}\n  return {index}[{parameter}] ?? {}\n}}\n",
                render_type(key_type)?,
                render_type(value_type)?,
                render_value(fallback, 2)?,
                render_value(fallback, 2)?
            )?;
        }
    }
    Ok(())
}

/// Render a language-neutral typegen module as TypeScript.
fn render_typegen_module(module: &TypegenModule) -> anyhow::Result<String> {
    let mut out = String::from("// Auto-generated by typegen - do not edit manually\n\n");
    let mut names = BTreeSet::new();
    for declaration in &module.declarations {
        let name = match declaration {
            Declaration::TypeAlias { name, .. }
            | Declaration::Const { name, .. }
            | Declaration::FilteredArray { name, .. }
            | Declaration::Index { name, .. }
            | Declaration::Find { name, .. }
            | Declaration::LookupOr { name, .. } => name,
        };
        identifier(name)?;
        anyhow::ensure!(
            names.insert(name),
            "duplicate typegen declaration {name:?} in {}",
            module.path
        );
        render_declaration(&mut out, declaration)?;
    }
    Ok(out)
}
fn module_relative_path(path: &str) -> anyhow::Result<PathBuf> {
    let mut path = PathBuf::from(path);
    if path.extension().is_none() {
        path.set_extension("ts");
    }
    anyhow::ensure!(!path.is_absolute(), "typegen module path must be relative");
    anyhow::ensure!(
        path.components().all(|c| matches!(c, Component::Normal(_))),
        "typegen module path may not traverse directories: {}",
        path.display()
    );
    anyhow::ensure!(
        path.extension().is_some_and(|e| e == "ts"),
        "typegen module output must be a .ts file"
    );
    Ok(path)
}

fn format(path: &Path, text: String) -> anyhow::Result<String> {
    dprint_plugin_typescript::format_text(FormatTextOptions {
        path,
        extension: None,
        text,
        config: &ConfigurationBuilder::new().build(),
        external_formatter: None,
    })?
    .context("generated typegen module was empty")
}

pub(super) fn export_registered_typegen_modules(
    directory: &Path,
    crate_name: &str,
) -> anyhow::Result<()> {
    let mut modules = inventory::iter::<TypegenModuleRegistration>()
        .filter(|r| super::registration_belongs_to_crate(r.crate_name, crate_name))
        .map(|r| (r.id, (r.build)()))
        .collect::<Vec<_>>();
    modules.sort_by(|a, b| a.1.path.cmp(&b.1.path).then(a.0.cmp(b.0)));

    let mut paths = BTreeSet::new();
    let mut barrels: BTreeMap<PathBuf, BTreeSet<String>> = BTreeMap::new();
    for (id, module) in modules {
        let relative =
            module_relative_path(&module.path).with_context(|| format!("typegen module {id}"))?;
        anyhow::ensure!(
            paths.insert(relative.clone()),
            "duplicate typegen module output path: {}",
            relative.display()
        );
        let output = directory.join(&relative);
        if let Some(parent) = output.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&output, format(&output, render_typegen_module(&module)?)?)?;

        if module.barrels {
            let mut child = relative.with_extension("");
            while let Some(parent) = child.parent().filter(|p| !p.as_os_str().is_empty()) {
                let leaf = child
                    .file_name()
                    .and_then(|s| s.to_str())
                    .context("non-UTF8 typegen path")?;
                barrels
                    .entry(parent.join("index.ts"))
                    .or_default()
                    .insert(format!("export * from {}", string(&format!("./{leaf}"))?));
                child = parent.to_path_buf();
            }
        }
    }
    for (relative, exports) in barrels {
        anyhow::ensure!(
            !paths.contains(&relative),
            "SDK barrel conflicts with module: {}",
            relative.display()
        );
        let output = directory.join(relative);
        let text = format!(
            "// Auto-generated by typegen - do not edit manually\n{}\n",
            exports.into_iter().collect::<Vec<_>>().join("\n")
        );
        fs::write(&output, format(&output, text)?)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::typegen_module::{Field, Operand, Predicate};

    #[test]
    fn renders_catalog_deterministically_and_escapes_data() {
        let node = Type::named("NodeDef");
        let module = TypegenModule::new("bindingNode/nodes/types")
            .declare(Declaration::type_alias(
                "NodeCategory",
                Type::StringUnion(vec!["scene-control".into(), "quote'\\\n".into()]),
            ))
            .declare(Declaration::type_alias(
                "NodeDef",
                Type::Object(vec![
                    Field::new("typeId", Type::String),
                    Field::new("hiddenFromPalette", Type::Boolean).optional(),
                ]),
            ))
            .declare(Declaration::constant(
                "SceneNode",
                node.clone(),
                Value::Object(vec![("typeId".into(), Value::string("scene\\\"node"))]),
            ))
            .declare(Declaration::constant(
                "allNodeDefs",
                Type::array(node.clone()),
                Value::Array(vec![Value::reference("SceneNode")]),
            ))
            .declare(Declaration::FilteredArray {
                name: "dynamicNodeDefs".into(),
                source: "allNodeDefs".into(),
                parameter: "x".into(),
                predicate: Predicate::NotEqual(
                    Operand::ParameterField("typeId".into()),
                    Operand::String("hidden".into()),
                ),
            })
            .declare(Declaration::Index {
                name: "nodeDefsByType".into(),
                source: "allNodeDefs".into(),
                key_field: "typeId".into(),
                value_type: node.clone(),
            })
            .declare(Declaration::Find {
                name: "findNodeDef".into(),
                index: "nodeDefsByType".into(),
                parameter: "typeId".into(),
                key_type: Type::String,
                value_type: node,
            });
        let first = render_typegen_module(&module);
        let second = render_typegen_module(&module);
        assert!(first.is_ok());
        assert!(second.is_ok());
        let (Ok(first), Ok(second)) = (first, second) else {
            return;
        };
        assert_eq!(first, second);
        assert!(first.contains(r#""scene-control""#));
        assert!(first.contains("quote'"));
        assert!(first.contains("Object.fromEntries"));
        assert!(first.contains("return nodeDefsByType[typeId]"));
    }

    #[test]
    fn rejects_duplicate_declarations_and_unsafe_paths() {
        let module = TypegenModule::new("x")
            .declare(Declaration::type_alias("Same", Type::String))
            .declare(Declaration::type_alias("Same", Type::String));
        assert!(matches!(
            render_typegen_module(&module),
            Err(error) if error.to_string().contains("duplicate")
        ));
        assert!(module_relative_path("../escape").is_err());
    }
}
