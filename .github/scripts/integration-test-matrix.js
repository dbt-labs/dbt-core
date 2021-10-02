module.exports = ({ context }) => {
  const defaultPythonVersion = "3.8";
  const supportedPythonVersions = ["3.6", "3.7", "3.8", "3.9"];
  const supportedAdapters = ["postgres"];

  // if PR, generate matrix based on files changed and PR labels
  if (context.eventName.includes("pull_request")) {
    // `changes` is a list of adapter names that have related
    // file changes in the PR
    // ex: ['postgres', 'snowflake']
    const changes = JSON.parse(process.env.CHANGES);
    const labels = context.payload.pull_request.labels.map(({ name }) => name);
    console.log("labels", labels);
    console.log("changes", changes);
    const testAllLabel = labels.includes("test all");
    const include = [];

    for (const adapter of supportedAdapters) {
      if (
        changes.includes(adapter) ||
        testAllLabel ||
        labels.includes(`test ${adapter}`)
      ) {
        for (const pythonVersion of supportedPythonVersions) {
          if (
            pythonVersion === defaultPythonVersion ||
            labels.includes(`test python${pythonVersion}`) ||
            testAllLabel
          ) {
            // always run tests on ubuntu by default
            include.push({
              os: "ubuntu-latest",
              adapter,
              "python-version": pythonVersion,
            });

            if (labels.includes("test windows") || testAllLabel) {
              include.push({
                os: "windows-latest",
                adapter,
                "python-version": pythonVersion,
              });
            }

            if (labels.includes("test macos") || testAllLabel) {
              include.push({
                os: "macos-latest",
                adapter,
                "python-version": pythonVersion,
              });
            }
          }
        }
      }
    }

    console.log("matrix", { include });

    return {
      include,
    };
  }
  // if not PR, generate matrix of python version, adapter, and operating
  // system to run integration tests on

  const include = [];
  // run for all adapters and python versions on ubuntu
  for (const adapter of supportedAdapters) {
    for (const pythonVersion of supportedPythonVersions) {
      include.push({
        os: 'ubuntu-latest',
        adapter: adapter,
        "python-version": pythonVersion,
      });
    }
  }

  // additionally include runs for all adapters, on macos and windows,
  // but only for the default python version
  for (const adapter of supportedAdapters) {
    for (const operatingSystem of ["windows-latest", "macos-latest"]) {
      include.push({
        os: operatingSystem,
        adapter: adapter,
        "python-version": defaultPythonVersion,
      });
    }
  }

  console.log("matrix", { include });

  return {
    include,
  };
};
