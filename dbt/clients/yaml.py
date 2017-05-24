import dbt.compat
import dbt.exceptions

import yaml


def line_no(i, line, width=3):
    line_number = dbt.compat.to_string(i).ljust(width)
    return "{}| {}".format(line_number, line)


def prefix_with_line_numbers(line_list, starting_number):
    numbers = range(starting_number, starting_number + len(line_list))

    lines = [line_no(i, line) for (i, line) in zip(numbers, line_list)]
    return "\n".join(lines)


def contextualized_yaml_erro(raw_contents, error):
    mark = error.problem_mark

    line = mark.line
    human_line = line + 1

    line_list = raw_contents.split('\n')

    min_line = max(line - 3, 0)
    max_line = line + 3

    relevant_lines = line_list[min_line:max_line]
    lines = prefix_with_line_numbers(relevant_lines, min_line + 1)

    output = [
        "Syntax error near line {}".format(human_line),
        "-" * 30,
        lines,
        "\nRaw Error:",
        "-" * 30,
        dbt.compat.to_string(error)
    ]

    return "\n".join(output)


def load_yaml_text(contents):
    try:
        return yaml.safe_load(contents)
    except (yaml.scanner.ScannerError, yaml.YAMLError) as e:
        if hasattr(e, 'problem_mark'):
            error = contextualized_yaml_erro(contents, e)
        else:
            error = dbt.compat.to_string(e)

        raise dbt.exceptions.ValidationException(error)
