import re

from dbt.exceptions import VersionsNotCompatibleException
import dbt.utils

_MATCHERS = "(?P<matcher>\>=|\>|\<|\<=|=)?"
_NUM_NO_LEADING_ZEROS = "(0|[1-9][0-9]*)"
_ALPHA = "[0-9A-Za-z-]*"
_ALPHA_NO_LEADING_ZEROS = "(0|[1-9A-Za-z-][0-9A-Za-z-]*)"

_BASE_VERSION_REGEX = """
(?P<major>{num_no_leading_zeros})\.
(?P<minor>{num_no_leading_zeros})\.
(?P<patch>{num_no_leading_zeros})
""".format(num_no_leading_zeros=_NUM_NO_LEADING_ZEROS)

_VERSION_EXTRA_REGEX = """
(\-
  (?P<prerelease>
    {alpha_no_leading_zeros}(\.{alpha_no_leading_zeros})*))?
(\+
  (?P<build>
    {alpha}(\.{alpha})*))?
""".format(
    alpha_no_leading_zeros=_ALPHA_NO_LEADING_ZEROS,
    alpha=_ALPHA)

_VERSION_REGEX = re.compile("""
^
{matchers}
{base_version_regex}
{version_extra_regex}
$
""".format(
    matchers=_MATCHERS,
    base_version_regex=_BASE_VERSION_REGEX,
    version_extra_regex=_VERSION_EXTRA_REGEX),
                            re.VERBOSE)


class Matchers:
    GREATER_THAN = '>'
    GREATER_THAN_OR_EQUAL = '>='
    LESS_THAN = '<'
    LESS_THAN_OR_EQUAL = '<='
    EXACT = '='


class VersionRange(dbt.utils.AttrDict):

    def _try_combine_exact(self, a, b):
        if a.compare(b) == 0:
            return a
        else:
            raise VersionsNotCompatibleException()

    def _try_combine_lower_bound_with_exact(self, lower, exact):
        comparison = lower.compare(exact)

        if(comparison < 0 or
           (comparison == 0 and
            lower.matcher == Matchers.GREATER_THAN_OR_EQUAL)):
            return exact

        raise VersionsNotCompatibleException()

    def _try_combine_lower_bound(self, a, b):
        if b.is_unbounded:
            return a
        elif a.is_unbounded:
            return b

        if not (a.is_exact or b.is_exact):
            comparison = (a.compare(b) < 0)

            if comparison:
                return b
            else:
                return a

        elif a.is_exact:
            return self._try_combine_lower_bound_with_exact(b, a)

        elif b.is_exact:
            return self._try_combine_lower_bound_with_exact(a, b)

    def _try_combine_upper_bound_with_exact(self, upper, exact):
        comparison = upper.compare(exact)

        if(comparison > 0 or
           (comparison == 0 and
            upper.matcher == Matchers.LESS_THAN_OR_EQUAL)):
            return exact

        raise VersionsNotCompatibleException()

    def _try_combine_upper_bound(self, a, b):
        if b.is_unbounded:
            return a
        elif a.is_unbounded:
            return b

        if not (a.is_exact or b.is_exact):
            comparison = (a.compare(b) > 0)

            if comparison:
                return b
            else:
                return a

        elif a.is_exact:
            return self._try_combine_upper_bound_with_exact(b, a)

        elif b.is_exact:
            return self._try_combine_upper_bound_with_exact(a, b)

    def reduce(self, other):
        start = None

        if(self.start.is_exact and other.start.is_exact):
            start = end = self._try_combine_exact(self.start, other.start)

        else:
            start = self._try_combine_lower_bound(self.start, other.start)
            end = self._try_combine_upper_bound(self.end, other.end)

        if start.compare(end) > 0:
            raise VersionsNotCompatibleException()

        return VersionRange(start=start, end=end)

    def __str__(self):
        result = []

        if self.start.is_unbounded and self.end.is_unbounded:
            return 'ANY'

        if not self.start.is_unbounded:
            result.append(self.start.to_version_string())

        if not self.end.is_unbounded:
            result.append(self.end.to_version_string())

        return ', '.join(result)

    def to_version_string_pair(self):
        to_return = []

        if not self.start.is_unbounded:
            to_return.append(self.start.to_version_string())

        if not self.end.is_unbounded:
            to_return.append(self.end.to_version_string())

        return to_return


class VersionSpecifier(dbt.utils.AttrDict):

    def __init__(self, *args, **kwargs):
        super(VersionSpecifier, self).__init__(*args, **kwargs)

        if self.matcher is None:
            self.matcher = Matchers.EXACT

    def to_version_string(self, skip_matcher=False):
        prerelease = ''
        build = ''
        matcher = ''

        if self.prerelease:
            prerelease = '-' + self.prerelease

        if self.build:
            build = '+' + self.build

        if not skip_matcher:
            matcher = self.matcher
        return '{}{}.{}.{}{}{}'.format(
            matcher,
            self.major,
            self.minor,
            self.patch,
            prerelease,
            build)

    @classmethod
    def from_version_string(cls, version_string):
        match = _VERSION_REGEX.match(version_string)

        if match is None:
            # error?
            return None

        return VersionSpecifier(match.groupdict())

    def to_range(self):
        range_start = UnboundedVersionSpecifier()
        range_end = UnboundedVersionSpecifier()

        if self.matcher == Matchers.EXACT:
            range_start = self
            range_end = self

        elif self.matcher in [Matchers.GREATER_THAN,
                              Matchers.GREATER_THAN_OR_EQUAL]:
            range_start = self

        elif self.matcher in [Matchers.LESS_THAN,
                              Matchers.LESS_THAN_OR_EQUAL]:
            range_end = self

        return VersionRange(
            start=range_start,
            end=range_end)

    def compare(self, other):
        if self.is_unbounded or other.is_unbounded:
            return 0

        for key in ['major', 'minor', 'patch']:
            comparison = int(self[key]) - int(other[key])

            if comparison != 0:
                return comparison

        if((self.matcher == Matchers.GREATER_THAN_OR_EQUAL and
            other.matcher == Matchers.LESS_THAN_OR_EQUAL) or
           (self.matcher == Matchers.LESS_THAN_OR_EQUAL and
            other.matcher == Matchers.GREATER_THAN_OR_EQUAL)):
            return 0

        if((self.matcher == Matchers.LESS_THAN and
            other.matcher == Matchers.LESS_THAN_OR_EQUAL) or
           (other.matcher == Matchers.GREATER_THAN and
            self.matcher == Matchers.GREATER_THAN_OR_EQUAL) or
           (self.is_upper_bound and other.is_lower_bound)):
            return -1

        if((other.matcher == Matchers.LESS_THAN and
            self.matcher == Matchers.LESS_THAN_OR_EQUAL) or
           (self.matcher == Matchers.GREATER_THAN and
            other.matcher == Matchers.GREATER_THAN_OR_EQUAL) or
           (self.is_lower_bound and other.is_upper_bound)):
            return 1

        return 0

    @property
    def is_unbounded(self):
        return False

    @property
    def is_lower_bound(self):
        return self.matcher in [Matchers.GREATER_THAN,
                                Matchers.GREATER_THAN_OR_EQUAL]

    @property
    def is_upper_bound(self):
        return self.matcher in [Matchers.LESS_THAN,
                                Matchers.LESS_THAN_OR_EQUAL]

    @property
    def is_exact(self):
        return self.matcher == Matchers.EXACT


class UnboundedVersionSpecifier(VersionSpecifier):

    def __init__(self, *args, **kwargs):
        super(dbt.utils.AttrDict, self).__init__(*args, **kwargs)

    @property
    def is_unbounded(self):
        return True

    @property
    def is_lower_bound(self):
        return False

    @property
    def is_upper_bound(self):
        return False

    @property
    def is_exact(self):
        return False


def reduce_versions(*args, name=None):
    version_specifiers = []

    for version in args:
        if isinstance(version, UnboundedVersionSpecifier) or version is None:
            continue

        elif isinstance(version, VersionSpecifier):
            version_specifiers.append(version)

        elif isinstance(version, VersionRange):
            if not isinstance(version.start, UnboundedVersionSpecifier):
                version_specifiers.append(version.start)

            if not isinstance(version.end, UnboundedVersionSpecifier):
                version_specifiers.append(version.end)

        else:
            version_specifiers.append(
                VersionSpecifier.from_version_string(version))

    for version_specifier in version_specifiers:
        if not isinstance(version_specifier, VersionSpecifier):
            raise Exception(version_specifier)

    if not version_specifiers:
        return VersionRange(start=UnboundedVersionSpecifier(),
                            end=UnboundedVersionSpecifier())

    try:
        to_return = version_specifiers.pop().to_range()

        for version_specifier in version_specifiers:
            to_return = to_return.reduce(version_specifier.to_range())
    except VersionsNotCompatibleException as e:
        versions = [VersionSpecifier(arg).to_version_string() for arg in args]
        dbt.exceptions.incompatible_versions(name, versions)

    return to_return


def versions_compatible(*args):
    if len(args) == 1:
        return True

    try:
        reduce_versions(*args)
        return True
    except VersionsNotCompatibleException as e:
        return False


def find_possible_versions(requested_range, available_versions):
    possible_versions = []

    for version_string in available_versions:
        version = VersionSpecifier.from_version_string(version_string)

        if(versions_compatible(version,
                               requested_range.start,
                               requested_range.end)):
            possible_versions.append(version_string)

    return possible_versions[::-1]


def resolve_to_specific_version(requested_range, available_versions):
    max_version = None
    max_version_string = None

    for version_string in available_versions:
        version = VersionSpecifier.from_version_string(version_string)

        if(versions_compatible(version,
                               requested_range.start,
                               requested_range.end) and
           (max_version is None or max_version.compare(version) < 0)):
            max_version = version
            max_version_string = version_string

    return max_version_string


def resolve_dependency_tree(version_index, unmet_dependencies, restrictions):
    for name, restriction in restrictions.items():
        if not versions_compatible(*restriction):
            raise VersionsNotCompatibleException('not compatible {}'.format(restriction))

    if not unmet_dependencies:
        return {}, {}

    to_return_tree = {}
    to_return_install = {}

    for dependency_name, version in unmet_dependencies.items():
        print('resolving path {}'.format(dependency_name))
        dependency_restrictions = reduce_versions(
            *restrictions.copy().get(dependency_name),
            name=dependency_name)

        possible_matches = find_possible_versions(
            dependency_restrictions,
            version_index[dependency_name].keys())

        for possible_match in possible_matches:
            print('reset with {} at {}'.format(dependency_name, possible_match))

            tree = {}
            install = {}
            new_restrictions = {}
            new_unmet_dependencies = {}

            match_found = False

            try:
                new_restrictions = restrictions.copy()
                new_restrictions[dependency_name] = reduce_versions(
                    dependency_restrictions,
                    possible_match,
                    name=dependency_name
                ).to_version_string_pair()

                recursive_version_info = version_index.get(dependency_name, {}).get(possible_match)
                new_unmet_dependencies = dbt.utils.deep_merge(
                    recursive_version_info.copy())

                print('new unmet dependencies')
                print(new_unmet_dependencies)

                new_restrictions = dbt.utils.deep_merge(
                    new_restrictions.copy(),
                    unmet_dependencies.copy(),
                    new_unmet_dependencies.copy())

                new_restrictions[dependency_name] += [possible_match]

                if dependency_name in new_unmet_dependencies:
                    del new_unmet_dependencies[dependency_name]

                for name, restriction in new_restrictions.items():
                    if not versions_compatible(*restriction):
                        raise VersionsNotCompatibleException('not compatible {}'.format(new_restrictions))

                else:
                    match_found = True

                    print('going down the stack with {}'.format(new_unmet_dependencies))
                    print('and {}'.format(install))
                    subtree, subinstall = resolve_dependency_tree(
                        version_index,
                        new_unmet_dependencies,
                        new_restrictions)

                    tree.update({
                        dependency_name: {
                            'version': possible_match,
                            'satisfies': [dependency_name],
                            'dependencies': subtree
                        }
                    })

                    install = dbt.utils.deep_merge(
                        install,
                        subinstall,
                        {dependency_name: possible_match})

                    print('then {}'.format(install))

                    to_return_tree = dbt.utils.deep_merge(
                        to_return_tree,
                        tree)

                    to_return_install = dbt.utils.deep_merge(
                        to_return_install,
                        install)

                    break

                if not match_found:
                    raise VersionsNotCompatibleException('No match found -- exhausted this part of the '
                                                         'tree.')

            except VersionsNotCompatibleException as e:
                print(e)
                print('When attempting {} at {}'.format(dependency_name, possible_match))

    return to_return_tree.copy(), to_return_install.copy()


def resolve_dependency_set(version_index, dependencies):
    tree, install = resolve_dependency_tree(version_index, dependencies, dependencies)

    return {
        'install': install,
        'tree': tree,
    }
