import re
import sqlparse

from dbt.logger import GLOBAL_LOGGER as logger


class TokenType:
    DML = 'Token.Keyword.DML'
    CTE = 'Token.Keyword.CTE'

    @classmethod
    def match(cls, token, _type):
        return isinstance(token, sqlparse.sql.Token) and \
            str(token.ttype) == _type

    @classmethod
    def is_identifier(cls, token):
        return isinstance(token, sqlparse.sql.Identifier)

    @classmethod
    def is_dml(cls, token):
        return cls.match(token, cls.DML)

    @classmethod
    def is_cte(cls, token):
        return cls.match(token, cls.CTE)

    @classmethod
    def is_identifier_list(cls, token):
        return isinstance(token, sqlparse.sql.IdentifierList)


def parse_sql(sql):
    result = sqlparse.parse(sql)
    return result[0]


CTE_REGEX = re.compile('(?:with\s+)?(?P<name>\S*)\s+as\s+\((?P<sql>.*)\)', re.S)  # noqa
CREATE_AS_REGEX = re.compile('(?P<name>\S*)\s+as\s+with(?P<sql>.*)', re.S)  # noqa


def parse_cte_sql(sql):
    result = CTE_REGEX.search(sql)

    if result is None:
        return None

    return result.groups()


def get_ctes(sql):
    statement = parse_sql(sql)

    # ctes are tuples of (identifier, dml)
    result = {'ctes': [], 'dml': None}

    in_cte = False
    global_dml = []

    for token in statement:
        if TokenType.is_cte(token) and not in_cte:
            in_cte = True

        elif TokenType.is_dml(token) and in_cte:
            in_cte = False
            global_dml.append(token.value)

        elif TokenType.is_identifier(token) and in_cte:
            groups = parse_cte_sql(token.value)
            if groups is None:
                continue
            identifier, dml = groups
            parsed = get_ctes(dml)

            result['ctes'] = result['ctes'] + parsed['ctes']
            result['ctes'].append((identifier, parsed['dml'],))

        elif TokenType.is_identifier_list(token) and in_cte:
            for identifier_token in token.get_identifiers():
                groups = parse_cte_sql(identifier_token.value)
                if groups is None:
                    continue
                identifier, dml = groups
                parsed = get_ctes(dml)

                result['ctes'] += parsed['ctes']
                result['ctes'].append((identifier, parsed['dml'],))

        elif TokenType.is_identifier(token) and not in_cte:
            create_as_result = CREATE_AS_REGEX.search(token.value)

            if create_as_result is not None:
                name, sql = create_as_result.groups()

                global_dml.append('{} as'.format(name))

                identifier, dml = parse_cte_sql(sql)
                parsed = get_ctes(dml)

                result['ctes'] += parsed['ctes']
                result['ctes'].append((identifier, parsed['dml'],))
            else:
                global_dml.append(token.value)

        else:
            global_dml.append(token.value)

    result['dml'] = ''.join(global_dml).strip()

    return result


def hoist_ctes(sql):
    try:
        result = get_ctes(sql)
    except:
        import traceback
        traceback.print_exc()

    if not result['ctes']:
        return sql

    ctes_string = ', '.join(['{} as ({})'.format(identifier, dml)
                             for (identifier, dml) in result['ctes']])

    return 'with {} {}'.format(
        ctes_string,
        result['dml'])
