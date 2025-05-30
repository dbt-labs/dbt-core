use sprintf::parser::ConversionType;
use sprintf::{vsprintf, ConversionSpecifier, Printf, PrintfError};

use crate::error::{Error, ErrorKind};
use crate::value::{DynObject, ObjectRepr, Value, ValueKind, ValueRepr};

use super::mutable_vec;

const MIN_I128_AS_POS_U128: u128 = 170141183460469231731687303715884105728;

pub enum CoerceResult<'a> {
    I128(i128, i128),
    F64(f64, f64),
    Str(&'a str, &'a str),
}

pub(crate) fn as_f64(value: &Value, lossy: bool) -> Option<f64> {
    macro_rules! checked {
        ($expr:expr, $ty:ty) => {{
            let rv = $expr as f64;
            return if lossy || rv as $ty == $expr {
                Some(rv)
            } else {
                None
            };
        }};
    }

    Some(match value.0 {
        ValueRepr::Bool(x) => x as i64 as f64,
        ValueRepr::U64(x) => checked!(x, u64),
        ValueRepr::U128(x) => checked!(x.0, u128),
        ValueRepr::I64(x) => checked!(x, i64),
        ValueRepr::I128(x) => checked!(x.0, i128),
        ValueRepr::F64(x) => x,
        _ => return None,
    })
}

pub fn coerce<'x>(a: &'x Value, b: &'x Value, lossy: bool) -> Option<CoerceResult<'x>> {
    match (&a.0, &b.0) {
        // equal mappings are trivial
        (ValueRepr::U64(a), ValueRepr::U64(b)) => Some(CoerceResult::I128(*a as i128, *b as i128)),
        (ValueRepr::U128(a), ValueRepr::U128(b)) => {
            Some(CoerceResult::I128(a.0 as i128, b.0 as i128))
        }
        (ValueRepr::String(a, _), ValueRepr::String(b, _)) => Some(CoerceResult::Str(a, b)),
        (ValueRepr::SmallStr(a), ValueRepr::SmallStr(b)) => {
            Some(CoerceResult::Str(a.as_str(), b.as_str()))
        }
        (ValueRepr::SmallStr(a), ValueRepr::String(b, _)) => Some(CoerceResult::Str(a.as_str(), b)),
        (ValueRepr::String(a, _), ValueRepr::SmallStr(b)) => Some(CoerceResult::Str(a, b.as_str())),
        (ValueRepr::I64(a), ValueRepr::I64(b)) => Some(CoerceResult::I128(*a as i128, *b as i128)),
        (ValueRepr::I128(a), ValueRepr::I128(b)) => Some(CoerceResult::I128(a.0, b.0)),
        (ValueRepr::F64(a), ValueRepr::F64(b)) => Some(CoerceResult::F64(*a, *b)),

        // are floats involved?
        (ValueRepr::F64(a), _) => Some(CoerceResult::F64(*a, some!(as_f64(b, lossy)))),
        (_, ValueRepr::F64(b)) => Some(CoerceResult::F64(some!(as_f64(a, lossy)), *b)),

        // everything else goes up to i128
        _ => Some(CoerceResult::I128(
            some!(i128::try_from(a.clone()).ok()),
            some!(i128::try_from(b.clone()).ok()),
        )),
    }
}

fn get_offset_and_len<F: FnOnce() -> usize>(
    start: i64,
    stop: Option<i64>,
    end: F,
) -> (usize, usize) {
    if start < 0 || stop.map_or(true, |x| x < 0) {
        let end = end();
        let start = if start < 0 {
            (end as i64 + start) as usize
        } else {
            start as usize
        };
        let stop = match stop {
            None => end,
            Some(x) if x < 0 => (end as i64 + x) as usize,
            Some(x) => x as usize,
        };
        (start, stop.saturating_sub(start))
    } else {
        (
            start as usize,
            (stop.unwrap() as usize).saturating_sub(start as usize),
        )
    }
}

pub fn slice(value: Value, start: Value, stop: Value, step: Value) -> Result<Value, Error> {
    let start: i64 = if start.is_none() {
        0
    } else {
        ok!(start.try_into())
    };
    let stop = if stop.is_none() {
        None
    } else {
        Some(ok!(i64::try_from(stop)))
    };
    let step = if step.is_none() {
        1
    } else {
        ok!(u64::try_from(step)) as usize
    };
    if step == 0 {
        return Err(Error::new(
            ErrorKind::InvalidOperation,
            "cannot slice by step size of 0",
        ));
    }

    let kind = value.kind();
    let error = Err(Error::new(
        ErrorKind::InvalidOperation,
        format!("value of type {} cannot be sliced", kind),
    ));

    match value.0 {
        ValueRepr::String(..) | ValueRepr::SmallStr(_) => {
            let s = value.as_str().unwrap();
            let (start, len) = get_offset_and_len(start, stop, || s.chars().count());
            Ok(Value::from(
                s.chars()
                    .skip(start)
                    .take(len)
                    .step_by(step)
                    .collect::<String>(),
            ))
        }
        ValueRepr::Bytes(ref b) => {
            let (start, len) = get_offset_and_len(start, stop, || b.len());
            Ok(Value::from_bytes(
                b.get(start..start + len).unwrap_or_default().to_owned(),
            ))
        }
        ValueRepr::Undefined | ValueRepr::None => Ok(Value::from(Vec::<Value>::new())),
        ValueRepr::Object(obj) if matches!(obj.repr(), ObjectRepr::Seq | ObjectRepr::Iterable) => {
            let len = obj.enumerator_len().unwrap_or_default();
            let (start, len) = get_offset_and_len(start, stop, || len);

            // The manual matching here is important so that we don't mess up the size hint
            if let Some(iter) = obj.try_iter() {
                let sliced_iter = iter.skip(start).take(len).step_by(step).collect::<Vec<_>>();
                let mutable_vec = mutable_vec::MutableVec::from(sliced_iter);
                Ok(Value::from_object(mutable_vec))
            } else {
                Ok(Value::from(Vec::<Value>::new()))
            }
        }
        _ => error,
    }
}

fn int_as_value(val: i128) -> Value {
    if val as i64 as i128 == val {
        (val as i64).into()
    } else {
        val.into()
    }
}

fn impossible_op(op: &str, lhs: &Value, rhs: &Value) -> Error {
    Error::new(
        ErrorKind::InvalidOperation,
        format!(
            "tried to use {} operator on unsupported types {} and {}",
            op,
            lhs.kind(),
            rhs.kind()
        ),
    )
}

fn failed_op(op: &str, lhs: &Value, rhs: &Value) -> Error {
    Error::new(
        ErrorKind::InvalidOperation,
        format!("unable to calculate {lhs} {op} {rhs}"),
    )
}

/// A simple wrapper so we can implement `Printf` for our `Value`.
struct PrintfValue<'a>(&'a Value);

impl std::fmt::Display for PrintfValue<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Printf for PrintfValue<'_> {
    fn format(&self, spec: &ConversionSpecifier) -> Result<String, PrintfError> {
        match spec.conversion_type {
            ConversionType::DecInt => i64::try_from(self.0.clone())
                .map(|n| n.to_string())
                .map_err(|_| PrintfError::WrongType),
            ConversionType::DecFloatLower | ConversionType::DecFloatUpper => as_f64(self.0, true)
                .map(|n| n.to_string())
                .ok_or(PrintfError::WrongType),
            ConversionType::String | ConversionType::Char => Ok(self.0.to_string()),
            _ => Err(PrintfError::WrongType),
        }
    }

    fn as_int(&self) -> Option<i32> {
        i32::try_from(self.0.clone()).ok()
    }
}

/// Format a string with a slice of Value arguments using vsprintf
pub fn format_string(fmt: &str, args: &[Value]) -> Result<Value, Error> {
    let converted: Vec<PrintfValue> = args.iter().map(PrintfValue).collect();
    let printfs: Vec<&dyn Printf> = converted.iter().map(|v| v as &dyn Printf).collect();
    vsprintf(fmt, &printfs).map(Value::from).map_err(|e| {
        Error::new(
            ErrorKind::InvalidOperation,
            format!("Failed to format string: {}", e),
        )
    })
}

macro_rules! math_binop {
    ($name:ident, $int:ident, $float:tt) => {
        pub fn $name(lhs: &Value, rhs: &Value) -> Result<Value, Error> {
            match coerce(lhs, rhs, true) {
                Some(CoerceResult::I128(a, b)) => match a.$int(b) {
                    Some(val) => Ok(int_as_value(val)),
                    None => Err(failed_op(stringify!($float), lhs, rhs))
                },
                Some(CoerceResult::F64(a, b)) => Ok((a $float b).into()),
                _ => Err(impossible_op(stringify!($float), lhs, rhs))
            }
        }
    }
}

pub fn add(lhs: &Value, rhs: &Value) -> Result<Value, Error> {
    if matches!(lhs.kind(), ValueKind::Seq | ValueKind::Iterable)
        && matches!(rhs.kind(), ValueKind::Seq | ValueKind::Iterable)
    {
        let lhs = lhs.clone();
        let rhs = rhs.clone();

        if let Ok(lhs) = lhs.try_iter() {
            if let Ok(rhs) = rhs.try_iter() {
                let res = mutable_vec::MutableVec::from(lhs.chain(rhs).collect::<Vec<_>>());
                return Ok(Value::from(res));
            }
        }

        // This is really an error, so we don't return a mutable vec here.
        return Ok(Value::from(Vec::<Value>::new()));
    }
    match coerce(lhs, rhs, true) {
        Some(CoerceResult::I128(a, b)) => a
            .checked_add(b)
            .ok_or_else(|| failed_op("+", lhs, rhs))
            .map(int_as_value),
        Some(CoerceResult::F64(a, b)) => Ok((a + b).into()),
        Some(CoerceResult::Str(a, b)) => Ok(Value::from([a, b].concat())),
        _ => Err(impossible_op("+", lhs, rhs)),
    }
}

math_binop!(sub, checked_sub, -);
math_binop!(math_rem, checked_rem_euclid, %);

pub fn mul(lhs: &Value, rhs: &Value) -> Result<Value, Error> {
    if let Some((s, n)) = lhs
        .as_str()
        .map(|s| (s, rhs))
        .or_else(|| rhs.as_str().map(|s| (s, lhs)))
    {
        return Ok(Value::from(s.repeat(ok!(n.as_usize().ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidOperation,
                "strings can only be multiplied with integers",
            )
        })))));
    } else if let Some((seq, n)) = lhs
        .as_object()
        .map(|s| (s, rhs))
        .or_else(|| rhs.as_object().map(|s| (s, lhs)))
        .filter(|x| matches!(x.0.repr(), ObjectRepr::Iterable | ObjectRepr::Seq))
    {
        return repeat_iterable(n, seq);
    }

    match coerce(lhs, rhs, true) {
        Some(CoerceResult::I128(a, b)) => match a.checked_mul(b) {
            Some(val) => Ok(int_as_value(val)),
            None => Err(failed_op(stringify!(*), lhs, rhs)),
        },
        Some(CoerceResult::F64(a, b)) => Ok((a * b).into()),
        _ => Err(impossible_op(stringify!(*), lhs, rhs)),
    }
}

fn repeat_iterable(n: &Value, seq: &DynObject) -> Result<Value, Error> {
    struct LenIterWrap<I: Send + Sync>(usize, I);

    impl<I: Iterator<Item = Value> + Send + Sync> Iterator for LenIterWrap<I> {
        type Item = Value;

        #[inline(always)]
        fn next(&mut self) -> Option<Self::Item> {
            self.1.next()
        }

        #[inline(always)]
        fn size_hint(&self) -> (usize, Option<usize>) {
            (self.0, Some(self.0))
        }
    }

    let n = ok!(n.as_usize().ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidOperation,
            "sequences and iterables can only be multiplied with integers",
        )
    }));

    let len = ok!(seq.enumerator_len().ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidOperation,
            "cannot repeat unsized iterables",
        )
    }));

    // This is not optimal.  We only query the enumerator for the length once
    // but we support repeated iteration.  We could both lie about our length
    // here and we could actually deal with an object that changes how much
    // data it returns.  This is not really permissible so we won't try to
    // improve on this here.
    Ok(Value::make_object_iterable(seq.clone(), move |seq| {
        Box::new(LenIterWrap(
            len * n,
            (0..n).flat_map(move |_| {
                seq.try_iter().unwrap_or_else(|| {
                    Box::new(
                        std::iter::repeat(Value::from(Error::new(
                            ErrorKind::InvalidOperation,
                            "iterable did not iterate against expectations",
                        )))
                        .take(len),
                    )
                })
            }),
        ))
    }))
}

pub fn div(lhs: &Value, rhs: &Value) -> Result<Value, Error> {
    fn do_it(lhs: &Value, rhs: &Value) -> Option<Value> {
        let a = some!(as_f64(lhs, true));
        let b = some!(as_f64(rhs, true));
        Some((a / b).into())
    }
    do_it(lhs, rhs).ok_or_else(|| impossible_op("/", lhs, rhs))
}

pub fn int_div(lhs: &Value, rhs: &Value) -> Result<Value, Error> {
    match coerce(lhs, rhs, true) {
        Some(CoerceResult::I128(a, b)) => {
            if b != 0 {
                a.checked_div_euclid(b)
                    .ok_or_else(|| failed_op("//", lhs, rhs))
                    .map(int_as_value)
            } else {
                Err(failed_op("//", lhs, rhs))
            }
        }
        Some(CoerceResult::F64(a, b)) => Ok(a.div_euclid(b).into()),
        _ => Err(impossible_op("//", lhs, rhs)),
    }
}

/// Implements a binary `pow` operation on values.
pub fn pow(lhs: &Value, rhs: &Value) -> Result<Value, Error> {
    match coerce(lhs, rhs, true) {
        Some(CoerceResult::I128(a, b)) => {
            match TryFrom::try_from(b).ok().and_then(|b| a.checked_pow(b)) {
                Some(val) => Ok(int_as_value(val)),
                None => Err(failed_op("**", lhs, rhs)),
            }
        }
        Some(CoerceResult::F64(a, b)) => Ok((a.powf(b)).into()),
        _ => Err(impossible_op("**", lhs, rhs)),
    }
}

/// Implements an unary `neg` operation on value.
pub fn neg(val: &Value) -> Result<Value, Error> {
    if val.kind() == ValueKind::Number {
        match val.0 {
            ValueRepr::F64(x) => Ok((-x).into()),
            // special case for the largest i128 that can still be
            // represented.
            ValueRepr::U128(x) if x.0 == MIN_I128_AS_POS_U128 => {
                Ok(Value::from(MIN_I128_AS_POS_U128))
            }
            _ => {
                if let Ok(x) = i128::try_from(val.clone()) {
                    x.checked_mul(-1)
                        .ok_or_else(|| Error::new(ErrorKind::InvalidOperation, "overflow"))
                        .map(int_as_value)
                } else {
                    Err(Error::from(ErrorKind::InvalidOperation))
                }
            }
        }
    } else {
        Err(Error::from(ErrorKind::InvalidOperation))
    }
}

/// Attempts a string concatenation.
pub fn string_concat(left: Value, right: &Value) -> Value {
    Value::from(format!("{left}{right}"))
}

/// Implements a containment operation on values.
pub fn contains(container: &Value, value: &Value) -> Result<Value, Error> {
    // Special case where if the container is undefined, it cannot hold
    // values.  For strict containment checks the vm has a special case.
    if container.is_undefined() || container.is_none() {
        return Ok(Value::from(false));
    }
    let rv = if let Some(s) = container.as_str() {
        if let Some(s2) = value.as_str() {
            s.contains(s2)
        } else {
            s.contains(&value.to_string())
        }
    } else if let ValueRepr::Object(ref obj) = container.0 {
        match obj.repr() {
            ObjectRepr::Plain => false,
            ObjectRepr::Map => obj.get_value(value).is_some(),
            ObjectRepr::Seq | ObjectRepr::Iterable => {
                obj.try_iter().into_iter().flatten().any(|v| &v == value)
            }
        }
    } else {
        return Err(Error::new(
            ErrorKind::InvalidOperation,
            "cannot perform a containment check on this value",
        ));
    };
    Ok(Value::from(rv))
}

// New rem implementation that handles both string formatting and math remainder
pub fn rem(lhs: &Value, rhs: &Value) -> Result<Value, Error> {
    if matches!(lhs.kind(), ValueKind::String) {
        match rhs.kind() {
            ValueKind::Seq => {
                let seq: Vec<Value> = rhs
                    .try_iter()
                    .map_err(|e| {
                        Error::new(
                            ErrorKind::InvalidOperation,
                            format!("Expected sequence: {}", e),
                        )
                    })?
                    .collect::<Vec<_>>();
                format_string(lhs.as_str().unwrap(), &seq)
            }
            _ => format_string(lhs.as_str().unwrap(), &[rhs.clone()]),
        }
    } else {
        math_rem(lhs, rhs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use similar_asserts::assert_eq;

    #[test]
    fn test_neg() {
        let err = neg(&Value::from(i128::MIN)).unwrap_err();
        assert_eq!(err.to_string(), "invalid operation: overflow");
    }

    #[test]
    fn test_adding() {
        let err = add(&Value::from("a"), &Value::from(42)).unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid operation: tried to use + operator on unsupported types string and number"
        );

        assert_eq!(
            add(&Value::from(1), &Value::from(2)).unwrap(),
            Value::from(3)
        );
        assert_eq!(
            add(&Value::from("foo"), &Value::from("bar")).unwrap(),
            Value::from("foobar")
        );

        let err = add(&Value::from(i128::MAX), &Value::from(1)).unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid operation: unable to calculate 170141183460469231731687303715884105727 + 1"
        );
    }

    #[test]
    fn test_subtracting() {
        let err = sub(&Value::from("a"), &Value::from(42)).unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid operation: tried to use - operator on unsupported types string and number"
        );

        let err = sub(&Value::from("foo"), &Value::from("bar")).unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid operation: tried to use - operator on unsupported types string and string"
        );

        assert_eq!(
            sub(&Value::from(2), &Value::from(1)).unwrap(),
            Value::from(1)
        );
    }

    #[test]
    fn test_dividing() {
        let err = div(&Value::from("a"), &Value::from(42)).unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid operation: tried to use / operator on unsupported types string and number"
        );

        let err = div(&Value::from("foo"), &Value::from("bar")).unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid operation: tried to use / operator on unsupported types string and string"
        );

        assert_eq!(
            div(&Value::from(100), &Value::from(2)).unwrap(),
            Value::from(50.0)
        );

        let err = int_div(&Value::from(i128::MIN), &Value::from(-1i128)).unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid operation: unable to calculate -170141183460469231731687303715884105728 // -1"
        );
    }

    #[test]
    fn test_concat() {
        assert_eq!(
            string_concat(Value::from("foo"), &Value::from(42)),
            Value::from("foo42")
        );
        assert_eq!(
            string_concat(Value::from(23), &Value::from(42)),
            Value::from("2342")
        );
    }

    #[test]
    fn test_string_formatting() {
        #![allow(clippy::approx_constant)]

        // Test string formatting
        assert_eq!(
            format_string("%s", &[Value::from("world")]).unwrap(),
            Value::from("world")
        );

        // Test integer formatting
        assert_eq!(
            format_string("%d", &[Value::from(42)]).unwrap(),
            Value::from("42")
        );

        // Test float formatting
        assert_eq!(
            format_string("%f", &[Value::from(3.14)]).unwrap(),
            Value::from("3.14")
        );

        // Test multiple arguments
        assert_eq!(
            format_string(
                "%s %d %f",
                &[Value::from("test"), Value::from(42), Value::from(3.14)]
            )
            .unwrap(),
            Value::from("test 42 3.14")
        );

        // Test escaped percent
        assert_eq!(format_string("%%s", &[]).unwrap(), Value::from("%s"));

        assert_eq!(format_string("100%%", &[]).unwrap(), Value::from("100%"));

        // Test errors
        let err = format_string("%s %s", &[Value::from("one")]).unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid operation: Failed to format string: Too few arguments passed"
        );

        let err = format_string("%s", &[Value::from("one"), Value::from("two")]).unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid operation: Failed to format string: Too many arguments passed"
        );

        let err = format_string("%d", &[Value::from("not a number")]).unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid operation: Failed to format string: Incorrect type passed as an argument"
        );

        let err = format_string("%f", &[Value::from("not a number")]).unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid operation: Failed to format string: Incorrect type passed as an argument"
        );

        let err = format_string("%x", &[Value::from(42)]).unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid operation: Failed to format string: Incorrect type passed as an argument"
        );

        // Test string formatting with % operator
        assert_eq!(
            format_string(
                "Invalid value for on_schema_change (%s) specified. Setting default value of %s.",
                &[Value::from(""), Value::from("ignore")]
            )
            .unwrap(),
            Value::from(
                "Invalid value for on_schema_change () specified. Setting default value of ignore."
            )
        );
    }
}
