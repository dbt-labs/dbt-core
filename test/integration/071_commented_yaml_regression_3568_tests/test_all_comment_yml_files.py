from test.integration.base import DBTIntegrationTest, use_profile


class TestAllCommentYMLIsOk(DBTIntegrationTest):
    @property
    def schema(self):
        return "071_commented_yaml"

    @property
    def models(self):
        return "models"

    @use_profile('postgres')
    def test_postgres_parses_with_all_comment_yml(self):
        try:
            self.run_dbt(['parse'])
        except TypeError:
            assert False, '`dbt parse` failed with a yaml file that is all comments with the same exception as 3568'
        except:
            assert False, '`dbt parse` failed with a yaml file that is all comments'
