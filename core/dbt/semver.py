from dataclasses import dataclass
import re

from dbt.exceptions import VersionsNotCompatibleException
import dbt.utils

from dbt.dataclass_schema import dbtClassMixin, StrEnum
from typing import Optional


class Matchers(StrEnum):
    GREATER_THAN = '>'
    GREATER_THAN_OR_EQUAL = '>='
    LESS_THAN = '<'
    LESS_THAN_OR_EQUAL = '<='
    EXACT = '='


@dataclass
class VersionSpecification(dbtClassMixin):
    major: Optional[str] = None
    minor: Optional[str] = None
    patch: Optional[str] = None
    prerelease: Optional[str] = None
    build: Optional[str] = None
    matcher: Matchers = Matchers.EXACT


_MATCHERS = r"(?P<matcher>\>=|\>|\<|\<=|=)?"
_NUM_NO_LEADING_ZEROS = r"(0|[1-9][0-9]*)"
_ALPHA = r"[0-9A-Za-z-]*"
_ALPHA_NO_LEADING_ZEROS = r"(0|[1-9A-Za-z-][0-9A-Za-z-]*)"

_BASE_VERSION_REGEX = r"""
(?P<major>{num_no_leading_zeros})\.
(?P<minor>{num_no_leading_zeros})\.
(?P<patch>{num_no_leading_zeros})
""".format(num_no_leading_zeros=_NUM_NO_LEADING_ZEROS)

_VERSION_EXTRA_REGEX = r"""
(\-?
  (?P<prerelease>
    {alpha_no_leading_zeros}(\.{alpha_no_leading_zeros})*))?
(\+
  (?P<build>
    {alpha}(\.{alpha})*))?
""".format(
    alpha_no_leading_zeros=_ALPHA_NO_LEADING_ZEROS,
    alpha=_ALPHA)


_VERSION_REGEX_PAT_STR = r"""
^
{matchers}
{base_version_regex}
{version_extra_regex}
$
""".format(
    matchers=_MATCHERS,
    base_version_regex=_BASE_VERSION_REGEX,
    version_extra_regex=_VERSION_EXTRA_REGEX)

_VERSION_REGEX = re.compile(_VERSION_REGEX_PAT_STR, re.VERBOSE)


@dataclass
class VersionSpecifier(VersionSpecification):
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

        if not match:
            raise dbt.exceptions.SemverException(
                'Could not parse version "{}"'.format(version_string))

        matched = {k: v for k, v in match.groupdict().items() if v is not None}

        return cls.from_dict(matched)

    def __str__(self):
        return self.to_version_string()

    def to_range(self):
        range_start: VersionSpecifier = UnboundedVersionSpecifier()
        range_end: VersionSpecifier = UnboundedVersionSpecifier()

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
            comparison = int(getattr(self, key)) - int(getattr(other, key))

            if comparison > 0:
                return 1
            elif comparison < 0:
                return -1

        equal = ((self.matcher == Matchers.GREATER_THAN_OR_EQUAL and
                  other.matcher == Matchers.LESS_THAN_OR_EQUAL) or
                 (self.matcher == Matchers.LESS_THAN_OR_EQUAL and
                  other.matcher == Matchers.GREATER_THAN_OR_EQUAL))
        if equal:
            return 0

        lt = ((self.matcher == Matchers.LESS_THAN and
               other.matcher == Matchers.LESS_THAN_OR_EQUAL) or
              (other.matcher == Matchers.GREATER_THAN and
               self.matcher == Matchers.GREATER_THAN_OR_EQUAL) or
              (self.is_upper_bound and other.is_lower_bound))
        if lt:
            return -1

        gt = ((other.matcher == Matchers.LESS_THAN and
               self.matcher == Matchers.LESS_THAN_OR_EQUAL) or
              (self.matcher == Matchers.GREATER_THAN and
               other.matcher == Matchers.GREATER_THAN_OR_EQUAL) or
              (self.is_lower_bound and other.is_upper_bound))
        if gt:
            return 1

        return 0

    def __lt__(self, other):
        return self.compare(other) == -1

    def __gt__(self, other):
        return self.compare(other) == 1

    def __eq___(self, other):
        return self.compare(other) == 0

    def __cmp___(self, other):
        return self.compare(other)

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


@dataclass
class VersionRange:
    start: VersionSpecifier
    end: VersionSpecifier

    def _try_combine_exact(self, a, b):
        if a.compare(b) == 0:
            return a
        else:
            raise VersionsNotCompatibleException()

    def _try_combine_lower_bound_with_exact(self, lower, exact):
        comparison = lower.compare(exact)

        if (comparison < 0 or
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

        if (comparison > 0 or
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


class UnboundedVersionSpecifier(VersionSpecifier):
    def __init__(self, *args, **kwargs):
        super().__init__(
            matcher=Matchers.EXACT,
            major=None,
            minor=None,
            patch=None,
            prerelease=None,
            build=None
        )

    def __str__(self):
        return "*"

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


def reduce_versions(*args):
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
    except VersionsNotCompatibleException:
        raise VersionsNotCompatibleException(
            'Could not find a satisfactory version from options: {}'
            .format([str(a) for a in args]))

    return to_return


def versions_compatible(*args):
    if len(args) == 1:
        return True

    try:
        reduce_versions(*args)
        return True
    except VersionsNotCompatibleException:
        return False


def find_possible_versions(requested_range, available_versions):
    possible_versions = []

    for version_string in available_versions:
        version = VersionSpecifier.from_version_string(version_string)

        if(versions_compatible(version,
                               requested_range.start,
                               requested_range.end)):
            possible_versions.append(version)

    sorted_versions = sorted(possible_versions, reverse=True)
    return [v.to_version_string(skip_matcher=True) for v in sorted_versions]


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
