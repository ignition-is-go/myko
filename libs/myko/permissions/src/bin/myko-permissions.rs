use myko_permissions::{
    FilePermissionStorage, OperatorPrincipal, PermissionDocument, PermissionPrincipalKind,
    PermissionStorage,
};
use std::{collections::BTreeMap, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let operation = args
        .next()
        .ok_or("usage: myko-permissions bootstrap|recover|replace|show --store PATH [options]")?;
    let mut options = BTreeMap::new();
    let mut allow_lockout = false;
    while let Some(key) = args.next() {
        if key == "--allow-lockout" {
            allow_lockout = true;
            continue;
        }
        if !key.starts_with("--") {
            return Err("expected named option".into());
        }
        let value = args.next().ok_or("option value missing")?;
        if options.insert(key, value).is_some() {
            return Err("duplicate option".into());
        }
    }
    let path = options
        .remove("--store")
        .ok_or("--store PATH is required")?;
    let storage = FilePermissionStorage::new(PathBuf::from(path));
    if operation == "show" {
        if !options.is_empty() || allow_lockout {
            return Err("show accepts only --store".into());
        }
        println!("{}", serde_json::to_string_pretty(&storage.load()?)?);
        return Ok(());
    }
    let operator = options
        .remove("--operator")
        .ok_or("--operator identity is required")?;
    let reason = options.remove("--reason").ok_or("--reason is required")?;
    let result = match operation.as_str() {
        "bootstrap" | "recover" => {
            if allow_lockout {
                return Err("--allow-lockout is valid only for replace".into());
            }
            let principal = OperatorPrincipal {
                issuer: options.remove("--issuer").ok_or("--issuer is required")?,
                subject: options.remove("--subject").ok_or("--subject is required")?,
                kind: match options.remove("--kind").as_deref().unwrap_or("user") {
                    "user" => PermissionPrincipalKind::User,
                    "service" => PermissionPrincipalKind::Service,
                    "agent" => PermissionPrincipalKind::Agent,
                    _ => return Err("--kind must be user, service, or agent".into()),
                },
                name: options
                    .remove("--name")
                    .unwrap_or_else(|| "Operator-selected administrator".into()),
            };
            if !options.is_empty() {
                return Err("unknown option".into());
            }
            if operation == "bootstrap" {
                myko_permissions::bootstrap(&storage, principal, &operator, &reason)?
            } else {
                myko_permissions::recover(&storage, principal, &operator, &reason)?
            }
        }
        "replace" => {
            let path = options
                .remove("--document")
                .ok_or("--document PATH is required")?;
            let revision = options
                .remove("--expected-revision")
                .ok_or("--expected-revision is required")?
                .parse()?;
            if !options.is_empty() {
                return Err("unknown option".into());
            }
            let document: PermissionDocument = serde_json::from_reader(std::fs::File::open(path)?)?;
            myko_permissions::replace_as_operator(
                &storage,
                revision,
                document,
                &operator,
                &reason,
                allow_lockout,
            )?
        }
        _ => return Err("expected bootstrap, recover, replace, or show".into()),
    };
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}
