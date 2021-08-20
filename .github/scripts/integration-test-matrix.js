module.exports = ({ context }) => {
  if (context.eventName.includes("pull_request")) {
    const changes = JSON.parse(process.env.CHANGES);
    const labels = context.payload.pull_request.labels.map(({ name }) => name);
    console.log('labels', labels);
    console.log('changes', labels);

    const testAllLabel = labels.includes("test all");

    // PR matrix defaults
    const adapters = new Set();
    const pythonVersions = new Set(["3.8"]);
    const operatingSystems = new Set(["ubuntu-latest"]);

    for (const adapter of ["snowflake", "postgres", "bigquery", "redshift"]) {
      if (
        changes.includes(adapter) ||
        testAllLabel ||
        labels.includes(`test ${adapter}`)
      ) {
        adapters.add(adapter);
      }
    }

    for (const pythonVersion of ["3.6", "3.7", "3.8", "3.9"]) {
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
  }

  return {
    os: ["ubuntu-latest", "macos-latest", "windows-latest"],
    adapter: ["postgres", "snowflake", "bigquery", "redshift"],
    "python-version": ["3.6", "3.7", "3.8", "3.9"],
  };
};
