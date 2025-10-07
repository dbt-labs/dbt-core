use std::str::FromStr;

use crate::errors::{AdapterError, AdapterErrorKind};

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub enum DbrVersion {
    /// A NULL version
    #[default]
    Unset,
    /// An upper bound version like `16.x`
    Upper(i64),
    /// A full `{major}.{minor}` version like `16.2`
    Full(i64, i64),
}

impl DbrVersion {
    /// Creates a new `DbrVersion` with the given major and optional minor version.
    pub fn new(major: i64, minor: Option<i64>) -> Self {
        match minor {
            Some(minor) => Self::Full(major, minor),
            None => Self::Upper(major),
        }
    }
}

impl PartialOrd for DbrVersion {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DbrVersion {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        match (self, other) {
            (Self::Unset, Self::Unset) => Ordering::Equal,
            (Self::Upper(a1), Self::Upper(a2)) => a1.cmp(a2),
            (Self::Full(a1, b1), Self::Full(a2, b2)) => (a1, b1).cmp(&(a2, b2)),

            // unset is greater than any set version
            (Self::Unset, _) => Ordering::Greater,
            (_, Self::Unset) => Ordering::Less,

            // if A==B, then A.x > B.M for any M
            (Self::Upper(a1), Self::Full(a2, _)) => a1.cmp(a2).then(Ordering::Greater),
            (Self::Full(a1, _), Self::Upper(a2)) => a1.cmp(a2).then(Ordering::Less),
        }
    }
}

impl FromStr for DbrVersion {
    type Err = AdapterError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Parse the string as the result from `current_version().dbr_version`
        //
        // The version string is assumed to be in the format "{major}.{minor}" or "{major}.x".
        // We'll treat an empty string as unset.
        //
        // see https://github.com/databricks/dbt-databricks/blob/29199a7c8df568b53464fb1d88b8679a04d31fd4/dbt/adapters/databricks/handle.py#L269-L283
        // for the format of the version string.

        let Some(dbr_version) = s.split_whitespace().next() else {
            // split_whitespace always returns at least one element
            // unless the string is empty or is only whitespace.
            return Ok(Self::Unset);
        };

        let mut parts = dbr_version.split('.');

        let (Some(major), Some(minor)) = (parts.next(), parts.next()) else {
            return Err(AdapterError::new(
                AdapterErrorKind::Internal,
                format!("Invalid DBR version string: {dbr_version:?}"),
            ));
        };

        let major = major.parse().map_err(|_| {
            AdapterError::new(AdapterErrorKind::Internal, "Major version is not a number")
        })?;

        let minor = match minor {
            "x" => None, // "x" means any minor version
            other => Some(other.parse().map_err(|_| {
                AdapterError::new(AdapterErrorKind::Internal, "Minor version is not a number")
            })?),
        };

        Ok(Self::new(major, minor))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_unset() {
        let expected = DbrVersion::Unset;

        let actual: DbrVersion = "".parse().unwrap();
        assert_eq!(actual, expected);

        let actual: DbrVersion = " ".parse().unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_parse_major_only() {
        let expected = DbrVersion::new(16, None);

        let actual: DbrVersion = "16.x".parse().unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_parse_major_minor() {
        let expected = DbrVersion::new(16, Some(2));

        let actual: DbrVersion = "16.2".parse().unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_compare() {
        let unset = DbrVersion::default();
        let v16_0 = DbrVersion::new(16, Some(0));
        let v16_2 = DbrVersion::new(16, Some(2));
        let v16_3 = DbrVersion::new(16, Some(3));
        let v16_x = DbrVersion::new(16, None);
        let v17_0 = DbrVersion::new(17, Some(0));
        let v17_x = DbrVersion::new(17, None);

        for v in [v16_0, v16_2, v16_3, v16_x, v17_0, v17_x] {
            assert!(unset > v, "{unset:?} should be less than {v:?}");
            assert!(v < unset, "{v:?} should be greater than {unset:?}");
        }

        for v in [v16_0, v16_2, v16_3, v16_x, v17_0, v17_x].windows(2) {
            let [v1, v2] = v else { unreachable!() };
            assert!(v1 < v2, "{v1:?} should be less than {v2:?}");
            assert!(v2 > v1, "{v2:?} should be greater than {v1:?}");
        }

        assert!(v16_x < v17_x, "{v16_x:?} should be less than {v17_x:?}");
        assert!(v17_x > v16_x, "{v17_x:?} should be greater than {v16_x:?}");

        assert!(v16_x < v17_0, "{v16_x:?} should be less than {v17_0:?}");
        assert!(v17_0 > v16_x, "{v17_0:?} should be greater than {v16_x:?}");
    }
}
