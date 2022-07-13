from dbt.node_types import NodeType


class MetricReference(object):
    def __init__(self, metric_name, package_name=None):
        self.metric_name = metric_name
        self.package_name = package_name

    def __str__(self):
        return f"{self.metric_name}"


class ResolvedMetricReference(MetricReference):
    """
    Simple proxy over a ParsedMetric which delegates property
    lookups to the underlying node. Also adds helper functions
    for working with metrics (ie. __str__ and templating functions)
    """

    def __init__(self, node, manifest, Relation):
        super().__init__(node.name, node.package_name)
        self.node = node
        self.manifest = manifest
        self.Relation = Relation

    def __getattr__(self, key):
        return getattr(self.node, key)

    def __str__(self):
        return f"{self.node.name}"

    @classmethod
    def parent_metrics(cls, metric_node, manifest):
        yield metric_node

        for parent_unique_id in metric_node.depends_on.nodes:
            node = manifest.metrics.get(parent_unique_id)
            if node and node.resource_type == NodeType.Metric:
                yield from cls.parent_metrics(node, manifest)

    def parent_models(self):
        in_scope_metrics = list(self.parent_metrics(self.node, self.manifest))

        to_return = {
            "base": [],
            "derived": [],
        }
        for metric in in_scope_metrics:
            if metric.type == "expression":
                to_return["derived"].append(
                    {"metric_source": None, "metric": metric, "is_derived": True}
                )
            else:
                for node_unique_id in metric.depends_on.nodes:
                    node = self.manifest.nodes.get(node_unique_id)
                    if node and node.resource_type in NodeType.refable():
                        to_return["base"].append(
                            {
                                "metric_relation_node": node,
                                "metric_relation": self.Relation.create(
                                    database=node.database,
                                    schema=node.schema,
                                    identifier=node.alias,
                                ),
                                "metric": metric,
                                "is_derived": False,
                            }
                        )

        return to_return
