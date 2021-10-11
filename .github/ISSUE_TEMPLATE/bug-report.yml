name: 🐞 Bug
description: Report a bug or an issue you've found with dbt
title: "[Bug] <title>"
labels: ["bug", "triage"]
body:
  - type: markdown
    attributes:
      value: |
        Thanks for taking the time to fill out this bug report!
  - type: checkboxes
    attributes:
      label: Is there an existing issue for this?
      description: Please search to see if an issue already exists for the bug you encountered.
      options:
        - label: I have searched the existing issues
          required: true
  - type: textarea
    attributes:
      label: Current Behavior
      description: A concise description of what you're experiencing.
    validations:
      required: false
  - type: textarea
    attributes:
      label: Expected Behavior
      description: A concise description of what you expected to happen.
    validations:
      required: false
  - type: textarea
    attributes:
      label: Steps To Reproduce
      description: Steps to reproduce the behavior.
      placeholder: |
        1. In this environment...
        2. With this config...
        3. Run '...'
        4. See error...
    validations:
      required: false
  - type: textarea
    id: logs
    attributes:
      label: Relevant log output
      description: |
        If applicable, log output to help explain your problem.
      render: shell
    validations:
      required: false
  - type: textarea
    attributes:
      label: Environment
      description: |
        examples:
          - **OS**: Ubuntu 20.04
          - **Python**: 3.7.2 (`python --version`)
          - **dbt**: 0.21.0 (`dbt --version`)
      value: |
        - OS:
        - Python:
        - dbt:
      render: markdown
    validations:
      required: false
  - type: dropdown
    id: database
    attributes:
      label: What database are you using dbt with?
      multiple: true
      options:
        - postgres
        - redshift
        - snowflake
        - bigquery
        - other (mention it in "Additional Context")
    validations:
      required: false
  - type: textarea
    attributes:
      label: Additional Context
      description: |
        Links? References? Anything that will give us more context about the issue you are encountering!

        Tip: You can attach images or log files by clicking this area to highlight it and then dragging files in.
    validations:
      required: false
