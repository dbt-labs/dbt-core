#[cfg(test)]
mod tests {
    use dbt_schemas::schemas::packages::{TarballPackage, TarballPackageLock};
    use dbt_serde_yaml::Verbatim;
    use std::collections::HashMap;

    #[test]
    fn test_tarball_package_creation() {
        let tarball_package = TarballPackage {
            tarball: Verbatim("https://example.com/package.tar.gz".to_string()),
            unrendered: HashMap::new(),
        };

        assert_eq!(
            *tarball_package.tarball,
            "https://example.com/package.tar.gz"
        );
    }

    #[test]
    fn test_tarball_package_lock_creation() {
        let tarball_package_lock = TarballPackageLock {
            tarball: Verbatim("https://example.com/package.tar.gz".to_string()),
            name: "test-package".to_string(),
            unrendered: HashMap::new(),
        };

        assert_eq!(
            *tarball_package_lock.tarball,
            "https://example.com/package.tar.gz"
        );
        assert_eq!(tarball_package_lock.name, "test-package");
    }

    #[test]
    fn test_tarball_package_from_lock() {
        let tarball_package_lock = TarballPackageLock {
            tarball: Verbatim("https://example.com/package.tar.gz".to_string()),
            name: "test-package".to_string(),
            unrendered: HashMap::new(),
        };

        let tarball_package = TarballPackage::from(tarball_package_lock);
        assert_eq!(
            *tarball_package.tarball,
            "https://example.com/package.tar.gz"
        );
    }
}
