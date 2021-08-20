module.exports = ({ context }) => {
  const defaultPythonVersion = "3.8";
  const supportedPythonVersions = ["3.6", "3.7", "3.8", "3.9"];
  const supportedAdapters = ["snowflake", "postgres", "bigquery", "redshift"];

  if (context.eventName.includes("pull_request")) {
    // if PR, generate matrix based on files changed and PR labels
    const changes = JSON.parse(process.env.CHANGES);
    const labels = context.payload.pull_request.labels.map(({ name }) => name);
    console.log("labels", labels);
    console.log("changes", labels);

    const testAllLabel = labels.includes("test all");

    // PR matrix defaults
    const adapters = new Set();
    const pythonVersions = new Set([defaultPythonVersion]);
    const operatingSystems = new Set(["ubuntu-latest"]);

    for (const adapter of supportedAdapters) {
      if (
        changes.includes(adapter) ||
        testAllLabel ||
        labels.includes(`test ${adapter}`)
      ) {
        adapters.add(adapter);
      }
    }

    for (const pythonVersion of supportedPythonVersions) {
      if (labels.includes(`test python${pythonVersion}`) || testAllLabel) {
        pythonVersions.add(pythonVersion);
      }
    }

    if (labels.includes("test windows") || testAllLabel) {
      operatingSystems.add("windows-latest");
    }

    if (labels.includes("test linux") || testAllLabel) {
      operatingSystems.add("ubuntu-latest");
    }

    if (labels.includes("test macos") || testAllLabel) {
      operatingSystems.add("macos-latest");
    }

    return {
      os: [...operatingSystems],
      adapter: [...adapters],
      "python-version": [...pythonVersions],
    };
  } else if (context.eventName.includes("push")) {
    // if push, run a for all adapters and python versions on ubuntu
    // additionally include runs for all adapters, on other OS, but only for
    // the default python version
    const include = [];
    for (const adapter of supportedAdapters) {
      for (const operatingSystem of ["windows-latest", "macos-latest"]) {
        include.push({
          os: operatingSystem,
          adapter: adapter,
          "python-version": defaultPythonVersion,
        });
      }
    }

    return {
      os: ["ubuntu-latest"],
      adapter: supportedAdapters,
      "python-version": supportedPythonVersions,
      include,
    };
  }

  // otherwise (manual trigger, scheduled trigger, etc.) run everything
  return {
    os: ["ubuntu-latest", "windows-latest", "macos-latest"],
    adapter: supportedAdapters,
    "python-version": supportedPythonVersions,
  };
};
