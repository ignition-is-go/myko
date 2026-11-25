use std::{collections::BTreeMap, fmt, time::Instant};

use rquickjs::{Ctx, Exception, IntoJs, Object, Value, object::Property};
use serde::{
    Serialize, Serializer,
    ser::{
        SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
        SerializeTupleStruct, SerializeTupleVariant,
    },
};

#[derive(Debug)]
pub enum ConversionError {
    Js(rquickjs::Error),
    Unsupported(String),
}

impl fmt::Display for ConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Js(error) => error.fmt(f),
            Self::Unsupported(message) => message.fmt(f),
        }
    }
}
impl std::error::Error for ConversionError {}
impl serde::ser::Error for ConversionError {
    fn custom<T: fmt::Display>(message: T) -> Self {
        Self::Unsupported(message.to_string())
    }
}
impl From<rquickjs::Error> for ConversionError {
    fn from(error: rquickjs::Error) -> Self {
        Self::Js(error)
    }
}
impl ConversionError {
    pub fn into_js_error(self, ctx: &Ctx<'_>) -> rquickjs::Error {
        if ctx.has_exception() {
            return rquickjs::Error::Exception;
        }
        match self {
            Self::Js(error) => error,
            Self::Unsupported(message) => Exception::throw_message(ctx, &message),
        }
    }
}
type Result<T> = std::result::Result<T, ConversionError>;

pub(super) fn array_item<'js>(
    array: &rquickjs::Array<'js>,
    index: usize,
    value: Value<'js>,
) -> rquickjs::Result<()> {
    array.as_object().prop(
        index.to_string(),
        Property::from(value).writable().enumerable().configurable(),
    )
}

struct KeyOnly<'a, T: ?Sized>(&'a T);
impl<T: Serialize + ?Sized> Serialize for KeyOnly<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(self.0, &())?;
        map.end()
    }
}

struct DisplayWriter<'js> {
    sequence: Sequence<'js>,
    pending: String,
    error: Option<ConversionError>,
}
const DISPLAY_CHUNK_BYTES: usize = 4096;

impl DisplayWriter<'_> {
    fn flush(&mut self) -> Result<()> {
        self.sequence.converter.check()?;
        if !self.pending.is_empty() {
            self.sequence.element(&self.pending)?;
            self.pending.clear();
        }
        Ok(())
    }
}
impl fmt::Write for DisplayWriter<'_> {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        let result = (|| -> Result<()> {
            self.sequence.converter.check()?;
            for character in value.chars() {
                if self
                    .pending
                    .len()
                    .checked_add(character.len_utf8())
                    .is_none_or(|length| length > DISPLAY_CHUNK_BYTES)
                {
                    self.flush()?;
                }
                self.pending.push(character);
            }
            Ok(())
        })();
        if let Err(error) = result {
            self.error = Some(error);
            return Err(fmt::Error);
        }
        Ok(())
    }
}

struct RawJson<'js>(Converter<'js>);

macro_rules! reject_raw {
    ($method:ident $(, $kind:ty)?) => {
        fn $method(self $(, _: $kind)?) -> Result<Self::Ok> {
            Err(serde::ser::Error::custom("expected raw JSON string"))
        }
    };
}
impl<'js> Serializer for RawJson<'js> {
    type Ok = Value<'js>;
    type Error = ConversionError;
    type SerializeSeq = serde::ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTuple = Self::SerializeSeq;
    type SerializeTupleStruct = Self::SerializeSeq;
    type SerializeTupleVariant = Self::SerializeSeq;
    type SerializeMap = Self::SerializeSeq;
    type SerializeStruct = Self::SerializeSeq;
    type SerializeStructVariant = Self::SerializeSeq;
    reject_raw!(serialize_bool, bool);
    reject_raw!(serialize_i8, i8);
    reject_raw!(serialize_i16, i16);
    reject_raw!(serialize_i32, i32);
    reject_raw!(serialize_i64, i64);
    reject_raw!(serialize_u8, u8);
    reject_raw!(serialize_u16, u16);
    reject_raw!(serialize_u32, u32);
    reject_raw!(serialize_u64, u64);
    reject_raw!(serialize_f32, f32);
    reject_raw!(serialize_f64, f64);
    reject_raw!(serialize_char, char);
    reject_raw!(serialize_bytes, &[u8]);
    reject_raw!(serialize_none);
    reject_raw!(serialize_unit);
    reject_raw!(serialize_unit_struct, &'static str);
    fn serialize_str(self, text: &str) -> Result<Self::Ok> {
        use serde::de::DeserializeSeed;
        self.0.check()?;
        let mut parser = serde_json::Deserializer::from_str(text);
        let value = JsonSeed(self.0)
            .deserialize(&mut parser)
            .map_err(|error| ConversionError::Unsupported(error.to_string()))?;
        parser
            .end()
            .map_err(|error| ConversionError::Unsupported(error.to_string()))?;
        Ok(value)
    }
    fn serialize_some<T: Serialize + ?Sized>(self, _: &T) -> Result<Self::Ok> {
        Err(serde::ser::Error::custom("expected raw JSON string"))
    }
    fn serialize_unit_variant(self, _: &'static str, _: u32, _: &'static str) -> Result<Self::Ok> {
        Err(serde::ser::Error::custom("expected raw JSON string"))
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: &T,
    ) -> Result<Self::Ok> {
        Err(serde::ser::Error::custom("expected raw JSON string"))
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> Result<Self::Ok> {
        Err(serde::ser::Error::custom("expected raw JSON string"))
    }
    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq> {
        Err(serde::ser::Error::custom("expected raw JSON string"))
    }
    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple> {
        Err(serde::ser::Error::custom("expected raw JSON string"))
    }
    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        Err(serde::ser::Error::custom("expected raw JSON string"))
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        Err(serde::ser::Error::custom("expected raw JSON string"))
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap> {
        Err(serde::ser::Error::custom("expected raw JSON string"))
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct> {
        Err(serde::ser::Error::custom("expected raw JSON string"))
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Err(serde::ser::Error::custom("expected raw JSON string"))
    }
}

struct JsonSeed<'js>(Converter<'js>);
impl<'de, 'js> serde::de::DeserializeSeed<'de> for JsonSeed<'js> {
    type Value = Value<'js>;
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        parser: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        parser.deserialize_any(self)
    }
}
impl<'de, 'js> serde::de::Visitor<'de> for JsonSeed<'js> {
    type Value = Value<'js>;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("JSON value")
    }
    fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<Self::Value, E> {
        self.0.serialize_unit().map_err(E::custom)
    }
    fn visit_bool<E: serde::de::Error>(self, value: bool) -> std::result::Result<Self::Value, E> {
        self.0.serialize_bool(value).map_err(E::custom)
    }
    fn visit_i64<E: serde::de::Error>(self, value: i64) -> std::result::Result<Self::Value, E> {
        self.0.serialize_i64(value).map_err(E::custom)
    }
    fn visit_u64<E: serde::de::Error>(self, value: u64) -> std::result::Result<Self::Value, E> {
        self.0.serialize_u64(value).map_err(E::custom)
    }
    fn visit_f64<E: serde::de::Error>(self, value: f64) -> std::result::Result<Self::Value, E> {
        self.0.serialize_f64(value).map_err(E::custom)
    }
    fn visit_str<E: serde::de::Error>(self, value: &str) -> std::result::Result<Self::Value, E> {
        self.0.serialize_str(value).map_err(E::custom)
    }
    fn visit_seq<A: serde::de::SeqAccess<'de>>(
        self,
        mut access: A,
    ) -> std::result::Result<Self::Value, A::Error> {
        use serde::de::Error;
        let mut sequence = self.0.sequence(None).map_err(A::Error::custom)?;
        while let Some(value) = access.next_element_seed(JsonSeed(sequence.converter.clone()))? {
            array_item(&sequence.array, sequence.next, value).map_err(A::Error::custom)?;
            sequence.next = sequence
                .next
                .checked_add(1)
                .ok_or_else(|| A::Error::custom("array length overflow"))?;
        }
        sequence.finish().map_err(A::Error::custom)
    }
    fn visit_map<A: serde::de::MapAccess<'de>>(
        self,
        mut access: A,
    ) -> std::result::Result<Self::Value, A::Error> {
        use serde::de::Error;
        let mut mapping = self.0.object(None).map_err(A::Error::custom)?;
        while let Some(key) = access.next_key::<String>()? {
            let value = access.next_value_seed(JsonSeed(mapping.converter.clone()))?;
            mapping.values.insert(key, value);
        }
        mapping.finish().map_err(A::Error::custom)
    }
}

#[derive(Clone)]
pub struct Converter<'js> {
    ctx: Ctx<'js>,
    deadline: Option<Instant>,
}
impl<'js> Converter<'js> {
    pub const fn new(ctx: Ctx<'js>, deadline: Option<Instant>) -> Self {
        Self { ctx, deadline }
    }
    pub(super) fn check(&self) -> Result<()> {
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(Exception::throw_message(&self.ctx, "script deadline exceeded").into());
        }
        Ok(())
    }
    fn null(&self) -> Result<Value<'js>> {
        self.check()?;
        Ok(Value::new_null(self.ctx.clone()))
    }
    fn string(&self, value: &str) -> Result<Value<'js>> {
        self.check()?;
        Ok(value.into_js(&self.ctx)?)
    }
    fn sequence(self, outer: Option<(Object<'js>, &'static str)>) -> Result<Sequence<'js>> {
        self.check()?;
        Ok(Sequence {
            array: rquickjs::Array::new(self.ctx.clone())?,
            next: 0,
            converter: self,
            outer,
        })
    }
    fn object(self, outer: Option<(Object<'js>, &'static str)>) -> Result<Mapping<'js>> {
        self.check()?;
        Ok(Mapping {
            values: BTreeMap::new(),
            key: None,
            converter: self,
            outer,
            raw: false,
            raw_value: None,
        })
    }
}

pub struct Sequence<'js> {
    array: rquickjs::Array<'js>,
    next: usize,
    converter: Converter<'js>,
    outer: Option<(Object<'js>, &'static str)>,
}
impl<'js> Sequence<'js> {
    fn element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        self.converter.check()?;
        let value = value.serialize(self.converter.clone())?;
        array_item(&self.array, self.next, value)?;
        self.next = self
            .next
            .checked_add(1)
            .ok_or_else(|| ConversionError::Unsupported("array length overflow".into()))?;
        Ok(())
    }
    fn finish(self) -> Result<Value<'js>> {
        let value = self.array.into_value();
        if let Some((outer, variant)) = self.outer {
            outer.prop(
                variant,
                Property::from(value).writable().enumerable().configurable(),
            )?;
            Ok(outer.into_value())
        } else {
            Ok(value)
        }
    }
}
macro_rules! sequence_impl {
    ($trait:ident, $method:ident) => {
        impl<'js> $trait for Sequence<'js> {
            type Ok = Value<'js>;
            type Error = ConversionError;
            fn $method<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
                self.element(value)
            }
            fn end(self) -> Result<Self::Ok> {
                self.finish()
            }
        }
    };
}
sequence_impl!(SerializeSeq, serialize_element);
sequence_impl!(SerializeTuple, serialize_element);
sequence_impl!(SerializeTupleStruct, serialize_field);
sequence_impl!(SerializeTupleVariant, serialize_field);

pub struct Mapping<'js> {
    values: BTreeMap<String, Value<'js>>,
    key: Option<String>,
    converter: Converter<'js>,
    outer: Option<(Object<'js>, &'static str)>,
    raw: bool,
    raw_value: Option<Value<'js>>,
}
impl<'js> Mapping<'js> {
    fn field<T: Serialize + ?Sized>(&mut self, key: &'static str, value: &T) -> Result<()> {
        self.converter.check()?;
        if self.raw {
            if key != "$serde_json::private::RawValue" {
                return Err(serde::ser::Error::custom("invalid RawValue field"));
            }
            self.raw_value = Some(value.serialize(RawJson(self.converter.clone()))?);
            return Ok(());
        }
        let value = value.serialize(self.converter.clone())?;
        self.values.insert(key.into(), value);
        Ok(())
    }
    fn finish(self) -> Result<Value<'js>> {
        if self.raw {
            return self
                .raw_value
                .ok_or_else(|| serde::ser::Error::custom("missing RawValue field"));
        }
        let object = Object::new(self.converter.ctx.clone())?;
        for (key, value) in self.values {
            object.prop(
                key,
                Property::from(value).writable().enumerable().configurable(),
            )?;
        }
        let value = object.into_value();
        if let Some((outer, variant)) = self.outer {
            outer.prop(
                variant,
                Property::from(value).writable().enumerable().configurable(),
            )?;
            Ok(outer.into_value())
        } else {
            Ok(value)
        }
    }
}
impl<'js> SerializeMap for Mapping<'js> {
    type Ok = Value<'js>;
    type Error = ConversionError;
    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<()> {
        self.converter.check()?;
        let map = serde_json::to_value(KeyOnly(key))
            .map_err(|error| ConversionError::Unsupported(error.to_string()))?;
        self.key = map.as_object().and_then(|map| map.keys().next()).cloned();
        Ok(())
    }
    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        let key = self
            .key
            .take()
            .ok_or_else(|| ConversionError::Unsupported("missing map key".into()))?;
        let value = value.serialize(self.converter.clone())?;
        self.values.insert(key, value);
        Ok(())
    }
    fn end(self) -> Result<Self::Ok> {
        self.finish()
    }
}
impl<'js> SerializeStruct for Mapping<'js> {
    type Ok = Value<'js>;
    type Error = ConversionError;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        self.field(key, value)
    }
    fn end(self) -> Result<Self::Ok> {
        self.finish()
    }
}
impl<'js> SerializeStructVariant for Mapping<'js> {
    type Ok = Value<'js>;
    type Error = ConversionError;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        self.field(key, value)
    }
    fn end(self) -> Result<Self::Ok> {
        self.finish()
    }
}

macro_rules! number {
    ($method:ident, $kind:ty) => {
        fn $method(self, value: $kind) -> Result<Self::Ok> {
            self.check()?;
            Ok(value.into_js(&self.ctx)?)
        }
    };
}
impl<'js> Serializer for Converter<'js> {
    type Ok = Value<'js>;
    type Error = ConversionError;
    type SerializeSeq = Sequence<'js>;
    type SerializeTuple = Sequence<'js>;
    type SerializeTupleStruct = Sequence<'js>;
    type SerializeTupleVariant = Sequence<'js>;
    type SerializeMap = Mapping<'js>;
    type SerializeStruct = Mapping<'js>;
    type SerializeStructVariant = Mapping<'js>;

    number!(serialize_bool, bool);
    number!(serialize_i8, i8);
    number!(serialize_i16, i16);
    number!(serialize_i32, i32);
    number!(serialize_i64, i64);
    number!(serialize_u8, u8);
    number!(serialize_u16, u16);
    number!(serialize_u32, u32);
    fn serialize_u64(self, value: u64) -> Result<Self::Ok> {
        let value = serde_json::Number::from(value)
            .as_f64()
            .ok_or_else(|| ConversionError::Unsupported("JSON number out of range".into()))?;
        self.serialize_f64(value)
    }
    fn serialize_i128(self, value: i128) -> Result<Self::Ok> {
        if let Ok(value) = u64::try_from(value) {
            return self.serialize_u64(value);
        }
        if let Ok(value) = i64::try_from(value) {
            return self.serialize_i64(value);
        }
        Err(ConversionError::Unsupported(
            "JSON number out of range".into(),
        ))
    }
    fn serialize_u128(self, value: u128) -> Result<Self::Ok> {
        let value = u64::try_from(value)
            .map_err(|_| ConversionError::Unsupported("JSON number out of range".into()))?;
        self.serialize_u64(value)
    }
    fn serialize_f32(self, value: f32) -> Result<Self::Ok> {
        self.serialize_f64(f64::from(value))
    }
    fn serialize_f64(self, value: f64) -> Result<Self::Ok> {
        self.check()?;
        if value.is_finite() {
            Ok(Value::new_float(self.ctx, value))
        } else {
            self.null()
        }
    }
    fn serialize_char(self, value: char) -> Result<Self::Ok> {
        self.string(value.encode_utf8(&mut [0; 4]))
    }
    fn serialize_str(self, value: &str) -> Result<Self::Ok> {
        self.string(value)
    }
    fn serialize_bytes(self, value: &[u8]) -> Result<Self::Ok> {
        let mut sequence = self.sequence(None)?;
        for byte in value {
            sequence.element(byte)?;
        }
        sequence.finish()
    }
    fn serialize_none(self) -> Result<Self::Ok> {
        self.null()
    }
    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<Self::Ok> {
        value.serialize(self)
    }
    fn serialize_unit(self) -> Result<Self::Ok> {
        self.null()
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<Self::Ok> {
        self.null()
    }
    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
    ) -> Result<Self::Ok> {
        self.string(variant)
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        value: &T,
    ) -> Result<Self::Ok> {
        value.serialize(self)
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok> {
        let mut mapping = self.object(None)?;
        mapping.field(variant, value)?;
        mapping.finish()
    }
    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq> {
        self.sequence(None)
    }
    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple> {
        self.sequence(None)
    }
    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        self.sequence(None)
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        let outer = Object::new(self.ctx.clone())?;
        self.sequence(Some((outer, variant)))
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap> {
        self.object(None)
    }
    fn serialize_struct(self, name: &'static str, _: usize) -> Result<Self::SerializeStruct> {
        let mut mapping = self.object(None)?;
        mapping.raw = name == "$serde_json::private::RawValue";
        Ok(mapping)
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant> {
        let outer = Object::new(self.ctx.clone())?;
        self.object(Some((outer, variant)))
    }
    fn collect_str<T: fmt::Display + ?Sized>(self, value: &T) -> Result<Self::Ok> {
        use fmt::Write;
        let mut writer = DisplayWriter {
            sequence: self.sequence(None)?,
            pending: String::with_capacity(DISPLAY_CHUNK_BYTES),
            error: None,
        };
        if write!(&mut writer, "{value}").is_err() {
            return Err(writer
                .error
                .unwrap_or_else(|| serde::ser::Error::custom("display formatting failed")));
        }
        writer.flush()?;
        let array = writer.sequence.array;
        let join: rquickjs::Function = array.ctx().eval("chunks => { let text = ''; for (let i = 0; i < chunks.length; i++) text += chunks[i]; return text; }")?;
        Ok(join.call((array,))?)
    }
    fn is_human_readable(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests;
