from dbt.events.base_types import WarnLevel, InfoLevel, DebugLevel, ErrorLevel

from dbt.common.ui import warning_tag, line_wrap_message


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
# M - Deps generation
# =======================================================


class DepsScrubbedPackageName(WarnLevel):
    def code(self):
        return "M035"

    def message(self) -> str:
        return f"Detected secret env var in {self.package_name}. dbt will write a scrubbed representation to the lock file. This will cause issues with subsequent 'dbt deps' using the lock file, requiring 'dbt deps --upgrade'"
