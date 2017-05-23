import os.path
import yaml
import yaml.scanner

import dbt.exceptions
import dbt.compat

from dbt.logger import GLOBAL_LOGGER as logger


INVALID_PROFILE_MESSAGE = """
dbt encountered an error while trying to read your profiles.yml file:

{profiles_file}

Error:
{error_string}

{guess}
"""


def guess_yaml_error(raw_contents, mark):
    line, col = getattr(mark, 'line'), getattr(mark, 'column')
    if line is None or col is None:
        return ''

    line = int(line)
    col = int(col)

    lines = raw_contents.split('\n')

    context_up = "\n".join(lines[line-3:line])
    errant_line = lines[line] + " <---- There's yer problem"
    context_down = "\n".join(lines[line+1:line+3])

    output = [
        "-"*20,
        context_up,
        errant_line,
        context_down,
        "-"*20
    ]

    return "\n".join(output)

def read_profile(profiles_dir):
    path = os.path.join(profiles_dir, 'profiles.yml')

    contents = None
    if os.path.isfile(path):
        try:
            with open(path, 'r') as f:
                contents = f.read()
                return yaml.safe_load(contents)
        except (yaml.scanner.ScannerError, yaml.YAMLError) as e:
            if e.problem_mark is None:
                guess = ''
            else:
                guess = guess_yaml_error(contents, e.problem_mark)

            msg = INVALID_PROFILE_MESSAGE.format(
                    profiles_file=path,
                    error_string=dbt.compat.to_string(e),
                    guess=guess).strip()
            raise dbt.exceptions.ValidationException(msg)

    return {}


def read_config(profiles_dir):
    profile = read_profile(profiles_dir)
    return profile.get('config', {})


def send_anonymous_usage_stats(config):
    return config.get('send_anonymous_usage_stats', True)


def colorize_output(config):
    return config.get('use_colors', True)
