from dbt.constants import MAXIMUM_SEED_SIZE_NAME, PIN_PACKAGE_URL
from dbt.events.base_types import WarnLevel, InfoLevel, DebugLevel, ErrorLevel

from dbt.common.ui import warning_tag, line_wrap_message, yellow


# =======================================================
# A - Pre-project loading
# =======================================================


class MainReportVersion(InfoLevel):
    def code(self) -> str:
        return "A001"

    def message(self) -> str:
        return f"Running with dbt{self.version}"


class MainReportArgs(DebugLevel):
    def code(self) -> str:
        return "A002"

    def message(self) -> str:
        return f"running dbt with arguments {str(self.args)}"


class MainTrackingUserState(DebugLevel):
    def code(self) -> str:
        return "A003"

    def message(self) -> str:
        return f"Tracking: {self.user_state}"


class MergedFromState(DebugLevel):
    def code(self) -> str:
        return "A004"

    def message(self) -> str:
        return f"Merged {self.num_merged} items from state (sample: {self.sample})"


class MissingProfileTarget(InfoLevel):
    def code(self) -> str:
        return "A005"

    def message(self) -> str:
        return f"target not specified in profile '{self.profile_name}', using '{self.target_name}'"


# Skipped A006, A007


class InvalidOptionYAML(ErrorLevel):
    def code(self) -> str:
        return "A008"

    def message(self) -> str:
        return f"The YAML provided in the --{self.option_name} argument is not valid."


class LogDbtProjectError(ErrorLevel):
    def code(self) -> str:
        return "A009"

    def message(self) -> str:
        msg = "Encountered an error while reading the project:"
        if self.exc:
            msg += f"  ERROR: {str(self.exc)}"
        return msg


# Skipped A010


class LogDbtProfileError(ErrorLevel):
    def code(self) -> str:
        return "A011"

    def message(self) -> str:
        msg = "Encountered an error while reading profiles:\n" f"  ERROR: {str(self.exc)}"
        if self.profiles:
            msg += "Defined profiles:\n"
            for profile in self.profiles:
                msg += f" - {profile}"
        else:
            msg += "There are no profiles defined in your profiles.yml file"

        msg += """
For more information on configuring profiles, please consult the dbt docs:

https://docs.getdbt.com/docs/configure-your-profile
"""
        return msg


class StarterProjectPath(DebugLevel):
    def code(self) -> str:
        return "A017"

    def message(self) -> str:
        return f"Starter project path: {self.dir}"


class ConfigFolderDirectory(InfoLevel):
    def code(self) -> str:
        return "A018"

    def message(self) -> str:
        return f"Creating dbt configuration folder at {self.dir}"


class NoSampleProfileFound(InfoLevel):
    def code(self) -> str:
        return "A019"

    def message(self) -> str:
        return f"No sample profile found for {self.adapter}."


class ProfileWrittenWithSample(InfoLevel):
    def code(self) -> str:
        return "A020"

    def message(self) -> str:
        return (
            f"Profile {self.name} written to {self.path} "
            "using target's sample configuration. Once updated, you'll be able to "
            "start developing with dbt."
        )


class ProfileWrittenWithTargetTemplateYAML(InfoLevel):
    def code(self) -> str:
        return "A021"

    def message(self) -> str:
        return (
            f"Profile {self.name} written to {self.path} using target's "
            "profile_template.yml and your supplied values. Run 'dbt debug' to "
            "validate the connection."
        )


class ProfileWrittenWithProjectTemplateYAML(InfoLevel):
    def code(self) -> str:
        return "A022"

    def message(self) -> str:
        return (
            f"Profile {self.name} written to {self.path} using project's "
            "profile_template.yml and your supplied values. Run 'dbt debug' to "
            "validate the connection."
        )


class SettingUpProfile(InfoLevel):
    def code(self) -> str:
        return "A023"

    def message(self) -> str:
        return "Setting up your profile."


class InvalidProfileTemplateYAML(InfoLevel):
    def code(self) -> str:
        return "A024"

    def message(self) -> str:
        return "Invalid profile_template.yml in project."


class ProjectNameAlreadyExists(InfoLevel):
    def code(self) -> str:
        return "A025"

    def message(self) -> str:
        return f"A project called {self.name} already exists here."


class ProjectCreated(InfoLevel):
    def code(self) -> str:
        return "A026"

    def message(self) -> str:
        return f"""
Your new dbt project "{self.project_name}" was created!

For more information on how to configure the profiles.yml file,
please consult the dbt documentation here:

  {self.docs_url}

One more thing:

Need help? Don't hesitate to reach out to us via GitHub issues or on Slack:

  {self.slack_url}

Happy modeling!
"""


# =======================================================
# D - Deprecations
# =======================================================


class DeprecatedModel(WarnLevel):
    def code(self) -> str:
        return "I065"

    def message(self) -> str:
        version = ".v" + self.model_version if self.model_version else ""
        msg = (
            f"Model {self.model_name}{version} has passed its deprecation date of {self.deprecation_date}. "
            "This model should be disabled or removed."
        )
        return warning_tag(msg)


class PackageRedirectDeprecation(WarnLevel):
    def code(self) -> str:
        return "D001"

    def message(self) -> str:
        description = (
            f"The `{self.old_name}` package is deprecated in favor of `{self.new_name}`. Please "
            f"update your `packages.yml` configuration to use `{self.new_name}` instead."
        )
        return line_wrap_message(warning_tag(f"Deprecated functionality\n\n{description}"))


class PackageInstallPathDeprecation(WarnLevel):
    def code(self) -> str:
        return "D002"

    def message(self) -> str:
        description = """\
        The default package install path has changed from `dbt_modules` to `dbt_packages`.
        Please update `clean-targets` in `dbt_project.yml` and check `.gitignore` as well.
        Or, set `packages-install-path: dbt_modules` if you'd like to keep the current value.
        """
        return line_wrap_message(warning_tag(f"Deprecated functionality\n\n{description}"))


class ConfigSourcePathDeprecation(WarnLevel):
    def code(self) -> str:
        return "D003"

    def message(self) -> str:
        description = (
            f"The `{self.deprecated_path}` config has been renamed to `{self.exp_path}`. "
            "Please update your `dbt_project.yml` configuration to reflect this change."
        )
        return line_wrap_message(warning_tag(f"Deprecated functionality\n\n{description}"))


class ConfigDataPathDeprecation(WarnLevel):
    def code(self) -> str:
        return "D004"

    def message(self) -> str:
        description = (
            f"The `{self.deprecated_path}` config has been renamed to `{self.exp_path}`. "
            "Please update your `dbt_project.yml` configuration to reflect this change."
        )
        return line_wrap_message(warning_tag(f"Deprecated functionality\n\n{description}"))


class MetricAttributesRenamed(WarnLevel):
    def code(self) -> str:
        return "D006"

    def message(self) -> str:
        description = (
            "dbt-core v1.3 renamed attributes for metrics:"
            "\n  'sql'              -> 'expression'"
            "\n  'type'             -> 'calculation_method'"
            "\n  'type: expression' -> 'calculation_method: derived'"
            f"\nPlease remove them from the metric definition of metric '{self.metric_name}'"
            "\nRelevant issue here: https://github.com/dbt-labs/dbt-core/issues/5849"
        )

        return warning_tag(f"Deprecated functionality\n\n{description}")


class ExposureNameDeprecation(WarnLevel):
    def code(self) -> str:
        return "D007"

    def message(self) -> str:
        description = (
            "Starting in v1.3, the 'name' of an exposure should contain only letters, "
            "numbers, and underscores. Exposures support a new property, 'label', which may "
            f"contain spaces, capital letters, and special characters. {self.exposure} does not "
            "follow this pattern. Please update the 'name', and use the 'label' property for a "
            "human-friendly title. This will raise an error in a future version of dbt-core."
        )
        return line_wrap_message(warning_tag(f"Deprecated functionality\n\n{description}"))


class InternalDeprecation(WarnLevel):
    def code(self) -> str:
        return "D008"

    def message(self) -> str:
        extra_reason = ""
        if self.reason:
            extra_reason = f"\n{self.reason}"
        msg = (
            f"`{self.name}` is deprecated and will be removed in dbt-core version {self.version}\n\n"
            f"Adapter maintainers can resolve this deprecation by {self.suggested_action}. {extra_reason}"
        )
        return warning_tag(msg)


class EnvironmentVariableRenamed(WarnLevel):
    def code(self) -> str:
        return "D009"

    def message(self) -> str:
        description = (
            f"The environment variable `{self.old_name}` has been renamed as `{self.new_name}`.\n"
            f"If `{self.old_name}` is currently set, its value will be used instead of `{self.new_name}`.\n"
            f"Set `{self.new_name}` and unset `{self.old_name}` to avoid this deprecation warning and "
            "ensure it works properly in a future release."
        )
        return line_wrap_message(warning_tag(f"Deprecated functionality\n\n{description}"))


class ConfigLogPathDeprecation(WarnLevel):
    def code(self) -> str:
        return "D010"

    def message(self) -> str:
        output = "logs"
        cli_flag = "--log-path"
        env_var = "DBT_LOG_PATH"
        description = (
            f"The `{self.deprecated_path}` config in `dbt_project.yml` has been deprecated, "
            f"and will no longer be supported in a future version of dbt-core. "
            f"If you wish to write dbt {output} to a custom directory, please use "
            f"the {cli_flag} CLI flag or {env_var} env var instead."
        )
        return line_wrap_message(warning_tag(f"Deprecated functionality\n\n{description}"))


class ConfigTargetPathDeprecation(WarnLevel):
    def code(self) -> str:
        return "D011"

    def message(self) -> str:
        output = "artifacts"
        cli_flag = "--target-path"
        env_var = "DBT_TARGET_PATH"
        description = (
            f"The `{self.deprecated_path}` config in `dbt_project.yml` has been deprecated, "
            f"and will no longer be supported in a future version of dbt-core. "
            f"If you wish to write dbt {output} to a custom directory, please use "
            f"the {cli_flag} CLI flag or {env_var} env var instead."
        )
        return line_wrap_message(warning_tag(f"Deprecated functionality\n\n{description}"))


# =======================================================
# I - Project parsing
# =======================================================


class InputFileDiffError(DebugLevel):
    def code(self) -> str:
        return "I001"

    def message(self) -> str:
        return f"Error processing file diff: {self.category}, {self.file_id}"


# Skipping I003, I004, I005, I006, I007


class InvalidValueForField(WarnLevel):
    def code(self) -> str:
        return "I008"

    def message(self) -> str:
        return f"Invalid value ({self.field_value}) for field {self.field_name}"


class ValidationWarning(WarnLevel):
    def code(self) -> str:
        return "I009"

    def message(self) -> str:
        return f"Field {self.field_name} is not valid for {self.resource_type} ({self.node_name})"


class ParsePerfInfoPath(InfoLevel):
    def code(self) -> str:
        return "I010"

    def message(self) -> str:
        return f"Performance info: {self.path}"


# Removed I011: GenericTestFileParse


# Removed I012: MacroFileParse


# Skipping I013


class PartialParsingErrorProcessingFile(DebugLevel):
    def code(self) -> str:
        return "I014"

    def message(self) -> str:
        return f"Partial parsing exception processing file {self.file}"


# Skipped I015


class PartialParsingError(DebugLevel):
    def code(self) -> str:
        return "I016"

    def message(self) -> str:
        return f"PP exception info: {self.exc_info}"


class PartialParsingSkipParsing(DebugLevel):
    def code(self) -> str:
        return "I017"

    def message(self) -> str:
        return "Partial parsing enabled, no changes found, skipping parsing"


# Skipped I018, I019, I020, I021, I022, I023


class UnableToPartialParse(InfoLevel):
    def code(self) -> str:
        return "I024"

    def message(self) -> str:
        return f"Unable to do partial parsing because {self.reason}"


class StateCheckVarsHash(DebugLevel):
    def code(self) -> str:
        return "I025"

    def message(self) -> str:
        return f"checksum: {self.checksum}, vars: {self.vars}, profile: {self.profile}, target: {self.target}, version: {self.version}"


# Skipped I025, I026, I026, I027


class PartialParsingNotEnabled(DebugLevel):
    def code(self) -> str:
        return "I028"

    def message(self) -> str:
        return "Partial parsing not enabled"


class ParsedFileLoadFailed(DebugLevel):
    def code(self) -> str:
        return "I029"

    def message(self) -> str:
        return f"Failed to load parsed file from disk at {self.path}: {self.exc}"


# Skipped I030-I039


class PartialParsingEnabled(DebugLevel):
    def code(self) -> str:
        return "I040"

    def message(self) -> str:
        return (
            f"Partial parsing enabled: "
            f"{self.deleted} files deleted, "
            f"{self.added} files added, "
            f"{self.changed} files changed."
        )


class PartialParsingFile(DebugLevel):
    def code(self) -> str:
        return "I041"

    def message(self) -> str:
        return f"Partial parsing: {self.operation} file: {self.file_id}"


# Skipped I042, I043, I044, I045, I046, I047, I048, I049


class InvalidDisabledTargetInTestNode(DebugLevel):
    def code(self) -> str:
        return "I050"

    def message(self) -> str:
        target_package_string = ""

        if self.target_package != target_package_string:
            target_package_string = f"in package '{self.target_package}' "

        msg = (
            f"{self.resource_type_title} '{self.unique_id}' "
            f"({self.original_file_path}) depends on a {self.target_kind} "
            f"named '{self.target_name}' {target_package_string}which is disabled"
        )

        return warning_tag(msg)


class UnusedResourceConfigPath(WarnLevel):
    def code(self) -> str:
        return "I051"

    def message(self) -> str:
        path_list = "\n".join(f"- {u}" for u in self.unused_config_paths)
        msg = (
            "Configuration paths exist in your dbt_project.yml file which do not "
            "apply to any resources.\n"
            f"There are {len(self.unused_config_paths)} unused configuration paths:\n{path_list}"
        )
        return warning_tag(msg)


class SeedIncreased(WarnLevel):
    def code(self) -> str:
        return "I052"

    def message(self) -> str:
        msg = (
            f"Found a seed ({self.package_name}.{self.name}) "
            f">{MAXIMUM_SEED_SIZE_NAME} in size. The previous file was "
            f"<={MAXIMUM_SEED_SIZE_NAME}, so it has changed"
        )
        return msg


class SeedExceedsLimitSamePath(WarnLevel):
    def code(self) -> str:
        return "I053"

    def message(self) -> str:
        msg = (
            f"Found a seed ({self.package_name}.{self.name}) "
            f">{MAXIMUM_SEED_SIZE_NAME} in size at the same path, dbt "
            f"cannot tell if it has changed: assuming they are the same"
        )
        return msg


class SeedExceedsLimitAndPathChanged(WarnLevel):
    def code(self) -> str:
        return "I054"

    def message(self) -> str:
        msg = (
            f"Found a seed ({self.package_name}.{self.name}) "
            f">{MAXIMUM_SEED_SIZE_NAME} in size. The previous file was in "
            f"a different location, assuming it has changed"
        )
        return msg


class SeedExceedsLimitChecksumChanged(WarnLevel):
    def code(self) -> str:
        return "I055"

    def message(self) -> str:
        msg = (
            f"Found a seed ({self.package_name}.{self.name}) "
            f">{MAXIMUM_SEED_SIZE_NAME} in size. The previous file had a "
            f"checksum type of {self.checksum_name}, so it has changed"
        )
        return msg


class UnusedTables(WarnLevel):
    def code(self) -> str:
        return "I056"

    def message(self) -> str:
        msg = [
            "During parsing, dbt encountered source overrides that had no target:",
        ]
        msg += self.unused_tables
        msg.append("")
        return warning_tag("\n".join(msg))


class WrongResourceSchemaFile(WarnLevel):
    def code(self) -> str:
        return "I057"

    def message(self) -> str:
        msg = line_wrap_message(
            f"""\
            '{self.patch_name}' is a {self.resource_type} node, but it is
            specified in the {self.yaml_key} section of
            {self.file_path}.
            To fix this error, place the `{self.patch_name}`
            specification under the {self.plural_resource_type} key instead.
            """
        )
        return warning_tag(msg)


class NoNodeForYamlKey(WarnLevel):
    def code(self) -> str:
        return "I058"

    def message(self) -> str:
        msg = (
            f"Did not find matching node for patch with name '{self.patch_name}' "
            f"in the '{self.yaml_key}' section of "
            f"file '{self.file_path}'"
        )
        return warning_tag(msg)


class MacroNotFoundForPatch(WarnLevel):
    def code(self) -> str:
        return "I059"

    def message(self) -> str:
        msg = f'Found patch for macro "{self.patch_name}" which was not found'
        return warning_tag(msg)


class NodeNotFoundOrDisabled(WarnLevel):
    def code(self) -> str:
        return "I060"

    def message(self) -> str:
        # this is duplicated logic from exceptions.get_not_found_or_disabled_msg
        # when we convert exceptions to be structured maybe it can be combined?
        # converting the bool to a string since None is also valid
        if self.disabled == "None":
            reason = "was not found or is disabled"
        elif self.disabled == "True":
            reason = "is disabled"
        else:
            reason = "was not found"

        target_package_string = ""

        if self.target_package is not None:
            target_package_string = f"in package '{self.target_package}' "

        msg = (
            f"{self.resource_type_title} '{self.unique_id}' "
            f"({self.original_file_path}) depends on a {self.target_kind} "
            f"named '{self.target_name}' {target_package_string}which {reason}"
        )

        return warning_tag(msg)


class JinjaLogWarning(WarnLevel):
    def code(self) -> str:
        return "I061"

    def message(self) -> str:
        return self.msg


class JinjaLogInfo(InfoLevel):
    def code(self) -> str:
        return "I062"

    def message(self) -> str:
        # This is for the log method used in macros so msg cannot be built here
        return self.msg


class JinjaLogDebug(DebugLevel):
    def code(self) -> str:
        return "I063"

    def message(self) -> str:
        # This is for the log method used in macros so msg cannot be built here
        return self.msg


class UnpinnedRefNewVersionAvailable(InfoLevel):
    def code(self) -> str:
        return "I064"

    def message(self) -> str:
        msg = (
            f"While compiling '{self.node_info.node_name}':\n"
            f"Found an unpinned reference to versioned model '{self.ref_node_name}' in project '{self.ref_node_package}'.\n"
            f"Resolving to latest version: {self.ref_node_name}.v{self.ref_node_version}\n"
            f"A prerelease version {self.ref_max_version} is available. It has not yet been marked 'latest' by its maintainer.\n"
            f"When that happens, this reference will resolve to {self.ref_node_name}.v{self.ref_max_version} instead.\n\n"
            f"  Try out v{self.ref_max_version}: {{{{ ref('{self.ref_node_package}', '{self.ref_node_name}', v='{self.ref_max_version}') }}}}\n"
            f"  Pin to  v{self.ref_node_version}: {{{{ ref('{self.ref_node_package}', '{self.ref_node_name}', v='{self.ref_node_version}') }}}}\n"
        )
        return msg


class UpcomingReferenceDeprecation(WarnLevel):
    def code(self) -> str:
        return "I066"

    def message(self) -> str:
        ref_model_version = ".v" + self.ref_model_version if self.ref_model_version else ""
        msg = (
            f"While compiling '{self.model_name}': Found a reference to {self.ref_model_name}{ref_model_version}, "
            f"which is slated for deprecation on '{self.ref_model_deprecation_date}'. "
        )

        if self.ref_model_version and self.ref_model_version != self.ref_model_latest_version:
            coda = (
                f"A new version of '{self.ref_model_name}' is available. Try it out: "
                f"{{{{ ref('{self.ref_model_package}', '{self.ref_model_name}', "
                f"v='{self.ref_model_latest_version}') }}}}."
            )
            msg = msg + coda

        return warning_tag(msg)


class DeprecatedReference(WarnLevel):
    def code(self) -> str:
        return "I067"

    def message(self) -> str:
        ref_model_version = ".v" + self.ref_model_version if self.ref_model_version else ""
        msg = (
            f"While compiling '{self.model_name}': Found a reference to {self.ref_model_name}{ref_model_version}, "
            f"which was deprecated on '{self.ref_model_deprecation_date}'. "
        )

        if self.ref_model_version and self.ref_model_version != self.ref_model_latest_version:
            coda = (
                f"A new version of '{self.ref_model_name}' is available. Migrate now: "
                f"{{{{ ref('{self.ref_model_package}', '{self.ref_model_name}', "
                f"v='{self.ref_model_latest_version}') }}}}."
            )
            msg = msg + coda

        return warning_tag(msg)


class UnsupportedConstraintMaterialization(WarnLevel):
    def code(self) -> str:
        return "I068"

    def message(self) -> str:
        msg = (
            f"Constraint types are not supported for {self.materialized} materializations and will "
            "be ignored.  Set 'warn_unsupported: false' on this constraint to ignore this warning."
        )

        return line_wrap_message(warning_tag(msg))


class ParseInlineNodeError(ErrorLevel):
    def code(self) -> str:
        return "I069"

    def message(self) -> str:
        return "Error while parsing node: " + self.node_info.node_name + "\n" + self.exc


class SemanticValidationFailure(WarnLevel):
    def code(self) -> str:
        return "I070"

    def message(self) -> str:
        return self.msg


class UnversionedBreakingChange(WarnLevel):
    def code(self) -> str:
        return "I071"

    def message(self) -> str:
        reasons = "\n  - ".join(self.breaking_changes)

        msg = (
            f"Breaking change to contracted, unversioned model {self.model_name} ({self.model_file_path})"
            "\nWhile comparing to previous project state, dbt detected a breaking change to an unversioned model."
            f"\n  - {reasons}\n"
        )

        return warning_tag(msg)


class WarnStateTargetEqual(WarnLevel):
    def code(self) -> str:
        return "I072"

    def message(self) -> str:
        return yellow(
            f"Warning: The state and target directories are the same: '{self.state_path}'. "
            f"This could lead to missing changes due to overwritten state including non-idempotent retries."
        )


class FreshnessConfigProblem(WarnLevel):
    def code(self) -> str:
        return "I073"

    def message(self) -> str:
        return self.msg


# =======================================================
# M - Deps generation
# =======================================================


class GitSparseCheckoutSubdirectory(DebugLevel):
    def code(self) -> str:
        return "M001"

    def message(self) -> str:
        return f"Subdirectory specified: {self.subdir}, using sparse checkout."


class GitProgressCheckoutRevision(DebugLevel):
    def code(self) -> str:
        return "M002"

    def message(self) -> str:
        return f"Checking out revision {self.revision}."


class GitProgressUpdatingExistingDependency(DebugLevel):
    def code(self) -> str:
        return "M003"

    def message(self) -> str:
        return f"Updating existing dependency {self.dir}."


class GitProgressPullingNewDependency(DebugLevel):
    def code(self) -> str:
        return "M004"

    def message(self) -> str:
        return f"Pulling new dependency {self.dir}."


class GitNothingToDo(DebugLevel):
    def code(self) -> str:
        return "M005"

    def message(self) -> str:
        return f"Already at {self.sha}, nothing to do."


class GitProgressUpdatedCheckoutRange(DebugLevel):
    def code(self) -> str:
        return "M006"

    def message(self) -> str:
        return f"Updated checkout from {self.start_sha} to {self.end_sha}."


class GitProgressCheckedOutAt(DebugLevel):
    def code(self) -> str:
        return "M007"

    def message(self) -> str:
        return f"Checked out at {self.end_sha}."


class RegistryProgressGETRequest(DebugLevel):
    def code(self) -> str:
        return "M008"

    def message(self) -> str:
        return f"Making package registry request: GET {self.url}"


class RegistryProgressGETResponse(DebugLevel):
    def code(self) -> str:
        return "M009"

    def message(self) -> str:
        return f"Response from registry: GET {self.url} {self.resp_code}"


class SelectorReportInvalidSelector(InfoLevel):
    def code(self) -> str:
        return "M010"

    def message(self) -> str:
        return (
            f"The '{self.spec_method}' selector specified in {self.raw_spec} is "
            f"invalid. Must be one of [{self.valid_selectors}]"
        )


class DepsNoPackagesFound(InfoLevel):
    def code(self) -> str:
        return "M013"

    def message(self) -> str:
        return "Warning: No packages were found in packages.yml"


class DepsStartPackageInstall(InfoLevel):
    def code(self) -> str:
        return "M014"

    def message(self) -> str:
        return f"Installing {self.package_name}"


class DepsInstallInfo(InfoLevel):
    def code(self) -> str:
        return "M015"

    def message(self) -> str:
        return f"Installed from {self.version_name}"


class DepsUpdateAvailable(InfoLevel):
    def code(self) -> str:
        return "M016"

    def message(self) -> str:
        return f"Updated version available: {self.version_latest}"


class DepsUpToDate(InfoLevel):
    def code(self) -> str:
        return "M017"

    def message(self) -> str:
        return "Up to date!"


class DepsListSubdirectory(InfoLevel):
    def code(self) -> str:
        return "M018"

    def message(self) -> str:
        return f"and subdirectory {self.subdirectory}"


class DepsNotifyUpdatesAvailable(InfoLevel):
    def code(self) -> str:
        return "M019"

    def message(self) -> str:
        return f"Updates available for packages: {self.packages} \
                \nUpdate your versions in packages.yml, then run dbt deps"


class RegistryIndexProgressGETRequest(DebugLevel):
    def code(self) -> str:
        return "M022"

    def message(self) -> str:
        return f"Making package index registry request: GET {self.url}"


class RegistryIndexProgressGETResponse(DebugLevel):
    def code(self) -> str:
        return "M023"

    def message(self) -> str:
        return f"Response from registry index: GET {self.url} {self.resp_code}"


class RegistryResponseUnexpectedType(DebugLevel):
    def code(self) -> str:
        return "M024"

    def message(self) -> str:
        return f"Response was None: {self.response}"


class RegistryResponseMissingTopKeys(DebugLevel):
    def code(self) -> str:
        return "M025"

    def message(self) -> str:
        # expected/actual keys logged in exception
        return f"Response missing top level keys: {self.response}"


class RegistryResponseMissingNestedKeys(DebugLevel):
    def code(self) -> str:
        return "M026"

    def message(self) -> str:
        # expected/actual keys logged in exception
        return f"Response missing nested keys: {self.response}"


class RegistryResponseExtraNestedKeys(DebugLevel):
    def code(self) -> str:
        return "M027"

    def message(self) -> str:
        # expected/actual keys logged in exception
        return f"Response contained inconsistent keys: {self.response}"


class DepsSetDownloadDirectory(DebugLevel):
    def code(self) -> str:
        return "M028"

    def message(self) -> str:
        return f"Set downloads directory='{self.path}'"


class DepsUnpinned(WarnLevel):
    def code(self) -> str:
        return "M029"

    def message(self) -> str:
        if self.revision == "HEAD":
            unpinned_msg = "not pinned, using HEAD (default branch)"
        elif self.revision in ("main", "master"):
            unpinned_msg = f'pinned to the "{self.revision}" branch'
        else:
            unpinned_msg = None

        msg = (
            f'The git package "{self.git}" \n\tis {unpinned_msg}.\n\tThis can introduce '
            f"breaking changes into your project without warning!\n\nSee {PIN_PACKAGE_URL}"
        )
        return yellow(f"WARNING: {msg}")


class NoNodesForSelectionCriteria(WarnLevel):
    def code(self) -> str:
        return "M030"

    def message(self) -> str:
        return f"The selection criterion '{self.spec_raw}' does not match any nodes"


class DepsLockUpdating(InfoLevel):
    def code(self):
        return "M031"

    def message(self) -> str:
        return f"Updating lock file in file path: {self.lock_filepath}"


class DepsAddPackage(InfoLevel):
    def code(self):
        return "M032"

    def message(self) -> str:
        return f"Added new package {self.package_name}@{self.version} to {self.packages_filepath}"


class DepsFoundDuplicatePackage(InfoLevel):
    def code(self):
        return "M033"

    def message(self) -> str:
        return f"Found duplicate package in packages.yml, removing: {self.removed_package}"


class DepsScrubbedPackageName(WarnLevel):
    def code(self):
        return "M035"

    def message(self) -> str:
        return f"Detected secret env var in {self.package_name}. dbt will write a scrubbed representation to the lock file. This will cause issues with subsequent 'dbt deps' using the lock file, requiring 'dbt deps --upgrade'"
