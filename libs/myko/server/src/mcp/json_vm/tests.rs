use super::*;

#[derive(Serialize)]
struct Unsorted {
    z: u8,
    a: u8,
}
#[derive(Serialize)]
enum Variant {
    Unit,
    Tuple(u8, String),
    Struct { z: u8, a: u8 },
}

struct DisplayValue;
impl fmt::Display for DisplayValue {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str("first")?;
        out.write_str("λ\nlast")
    }
}
impl Serialize for DisplayValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

struct Repeated;
impl Serialize for Repeated {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(5))?;
        map.serialize_entry("z", &1)?;
        map.serialize_entry("a", &2)?;
        map.serialize_entry("z", &3)?;
        map.serialize_entry("\u{10000}", &4)?;
        map.serialize_entry("\u{e000}", &5)?;
        map.end()
    }
}

struct FragmentedDisplay(usize);
impl fmt::Display for FragmentedDisplay {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        for _ in 0..self.0 {
            out.write_str("λ")?;
        }
        Ok(())
    }
}
impl Serialize for FragmentedDisplay {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

struct DisplayParts<'a>(&'a [&'a str]);
impl fmt::Display for DisplayParts<'_> {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        for part in self.0 {
            out.write_str(part)?;
        }
        Ok(())
    }
}
impl Serialize for DisplayParts<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

struct FormattingFailure;
impl fmt::Display for FormattingFailure {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str("partial")?;
        Err(fmt::Error)
    }
}
impl Serialize for FormattingFailure {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

#[test]
fn display_chunks_preserve_unicode_and_bound_native_scratch() -> anyhow::Result<()> {
    use fmt::Write;
    let runtime = rquickjs::Runtime::new()?;
    let context = rquickjs::Context::full(&runtime)?;
    context.with(|ctx| -> anyhow::Result<()> {
        let text = format!("{}🙂λ{}", "a".repeat(4095), "€".repeat(5000));
        let mut writer = DisplayWriter {
            sequence: Converter::new(ctx.clone(), None).sequence(None)?,
            pending: String::with_capacity(DISPLAY_CHUNK_BYTES),
            error: None,
        };
        writer.write_str("")?;
        writer.write_str(&text)?;
        anyhow::ensure!(writer.pending.capacity() == DISPLAY_CHUNK_BYTES);
        anyhow::ensure!(writer.sequence.next > 0);
        writer.flush()?;
        parity(&ctx, &DisplayParts(&[&text, "", "🙂λ"]))?;
        parity(&ctx, &DisplayParts(&["", ""]))?;
        Ok(())
    })
}

#[test]
fn display_formatting_failure_does_not_publish_partial_text() -> anyhow::Result<()> {
    let runtime = rquickjs::Runtime::new()?;
    let context = rquickjs::Context::full(&runtime)?;
    context.with(|ctx| -> anyhow::Result<()> {
        let result = FormattingFailure.serialize(Converter::new(ctx.clone(), None));
        anyhow::ensure!(matches!(result, Err(ConversionError::Unsupported(message)) if message == "display formatting failed"));
        Ok(())
    })
}

#[test]
fn fragmented_display_fits_the_same_vm_budget_as_legacy_json() -> anyhow::Result<()> {
    let runtime = rquickjs::Runtime::new()?;
    runtime.set_memory_limit(1024 * 1024);
    let context = rquickjs::Context::full(&runtime)?;
    context.with(|ctx| -> anyhow::Result<()> {
        let input = FragmentedDisplay(32768);
        let legacy = serde_json::to_value(&input)?;
        let expected = ctx.json_parse(serde_json::to_vec(&legacy)?)?;
        anyhow::ensure!(
            expected
                .as_string()
                .ok_or_else(|| anyhow::anyhow!("expected string"))?
                .to_string()?
                .len()
                == 65536
        );
        drop(expected);
        let actual = input.serialize(Converter::new(ctx.clone(), None))?;
        anyhow::ensure!(
            actual
                .as_string()
                .ok_or_else(|| anyhow::anyhow!("expected direct string"))?
                .to_string()?
                .len()
                == 65536
        );
        Ok(())
    })
}

struct Slow;
impl fmt::Display for Slow {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str("first")?;
        std::thread::sleep(std::time::Duration::from_millis(10));
        out.write_str("last")
    }
}
impl Serialize for Slow {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

fn parity<T: Serialize + ?Sized>(ctx: &Ctx<'_>, input: &T) -> anyhow::Result<()> {
    let legacy = serde_json::to_value(input);
    let direct = input.serialize(Converter::new(ctx.clone(), None));
    match (legacy, direct) {
        (Ok(legacy), Ok(direct)) => {
            let expected = ctx.json_parse(serde_json::to_vec(&legacy)?)?;
            let expected = ctx
                .json_stringify(expected)?
                .ok_or_else(|| anyhow::anyhow!("expected JSON missing"))?
                .to_string()?;
            let actual = ctx
                .json_stringify(direct)?
                .ok_or_else(|| anyhow::anyhow!("actual JSON missing"))?
                .to_string()?;
            anyhow::ensure!(actual == expected, "legacy={expected} direct={actual}");
        }
        (Err(_), Err(_)) => {}
        (legacy, direct) => {
            anyhow::bail!("legacy/direct success mismatch: {legacy:?} / {direct:?}")
        }
    }
    Ok(())
}

#[test]
fn preserves_json_values_keys_and_display_contract() -> anyhow::Result<()> {
    let runtime = rquickjs::Runtime::new()?;
    let context = rquickjs::Context::full(&runtime)?;
    context.with(|ctx| -> anyhow::Result<()> {
        parity(&ctx, &serde_json::json!({"null":null,"string":"λ\n\u{0}","values":[true,1.5]}))?;
        parity(&ctx, &Option::<u8>::None)?;
        parity(&ctx, &())?;
        parity(&ctx, &u64::MAX)?;
        parity(&ctx, &i64::MIN)?;
        parity(&ctx, &0.1_f32)?;
        parity(&ctx, &-0.0_f64)?;
        parity(&ctx, &f64::NAN)?;
        parity(&ctx, &f64::INFINITY)?;
        parity(&ctx, &u128::MAX)?;
        parity(&ctx, &i128::MIN)?;
        parity(&ctx, &9_007_199_254_740_993_u64)?;
        parity(&ctx, &Unsorted { z: 1, a: 2 })?;
        parity(&ctx, &Variant::Unit)?;
        parity(&ctx, &Variant::Tuple(1, "λ".into()))?;
        parity(&ctx, &Variant::Struct { z: 1, a: 2 })?;
        parity(&ctx, &DisplayValue)?;
        for raw in ["null", "{\"z\":1,\"a\":2,\"z\":3}", "[\"escaped\\nλ\",18446744073709551615]"] {
            parity(&ctx, &serde_json::value::RawValue::from_string(raw.into())?)?;
        }
        parity(&ctx, &BTreeMap::from([(true, 1)]))?;
        parity(&ctx, &BTreeMap::from([(u64::MAX, 1)]))?;
        parity(&ctx, &BTreeMap::from([(Some("bad"), 1)]))?;
        parity(&ctx, &KeyOnly(&-0.0_f32))?;
        parity(&ctx, &KeyOnly(&f64::NAN))?;
        parity(&ctx, &KeyOnly(&f64::INFINITY))?;
        parity(&ctx, &KeyOnly(&DisplayValue))?;
        parity(&ctx, &Repeated)?;
        let value = serde_json::json!({"__proto__":{"poison":true},"constructor":"own","prototype":"own"});
        parity(&ctx, &value)?;
        let direct = value.serialize(Converter::new(ctx.clone(), None))?;
        ctx.globals().set("value", direct)?;
        anyhow::ensure!(ctx.eval::<bool,_>("Object.prototype.poison===undefined && Object.getPrototypeOf(value)===Object.prototype && Object.prototype.hasOwnProperty.call(value,'__proto__')")?);
        ctx.globals().set("negative", (-0.0_f64).serialize(Converter::new(ctx.clone(), None))?)?;
        anyhow::ensure!(ctx.eval::<bool,_>("Object.is(negative,-0)")?);
        Ok(())
    })
}

#[test]
fn native_formatting_checks_deadline_without_a_vm_bytecode_loop() -> anyhow::Result<()> {
    let runtime = rquickjs::Runtime::new()?;
    let context = rquickjs::Context::full(&runtime)?;
    context.with(|ctx| -> anyhow::Result<()> {
        let deadline = Instant::now()
            .checked_add(std::time::Duration::from_millis(1))
            .ok_or_else(|| anyhow::anyhow!("test deadline overflow"))?;
        let result = Slow.serialize(Converter::new(ctx.clone(), Some(deadline)));
        anyhow::ensure!(result.is_err());
        anyhow::ensure!(ctx.has_exception());
        let error = ctx.catch();
        ctx.globals().set("error", error)?;
        anyhow::ensure!(ctx.eval::<String, _>("error.message")? == "script deadline exceeded");
        Ok(())
    })
}
