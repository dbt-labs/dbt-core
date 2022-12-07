from dbt.node_types import NodeType


class MetricReference(object):
    def __init__(self, metric_name, package_name=None):
        self.metric_name = metric_name
        self.package_name = package_name

    def __str__(self):
        return f"{self.metric_name}"


class ResolvedMetricReference(MetricReference):
    """
    Simple proxy over a Metric which delegates property
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

    @classmethod
    def parent_metrics_names(cls, metric_node, manifest):
        yield metric_node.name

        for parent_unique_id in metric_node.depends_on.nodes:
            node = manifest.metrics.get(parent_unique_id)
            if node and node.resource_type == NodeType.Metric:
                yield from cls.parent_metrics_names(node, manifest)

    @classmethod
    def reverse_dag_parsing(cls, metric_node, manifest, metric_depth_count):
        if metric_node.calculation_method == "derived":
            yield {metric_node.name: metric_depth_count}
            metric_depth_count = metric_depth_count + 1

        for parent_unique_id in metric_node.depends_on.nodes:
            node = manifest.metrics.get(parent_unique_id)
            if (
                node
                and node.resource_type == NodeType.Metric
                and node.calculation_method == "derived"
            ):
                yield from cls.reverse_dag_parsing(node, manifest, metric_depth_count)

    def full_metric_dependency(self):
        to_return = list(set(self.parent_metrics_names(self.node, self.manifest)))
        return to_return

    def base_metric_dependency(self):
        in_scope_metrics = list(self.parent_metrics(self.node, self.manifest))

        to_return = []
        for metric in in_scope_metrics:
            if metric.calculation_method != "derived" and metric.name not in to_return:
                to_return.append(metric.name)

        return to_return

    def derived_metric_dependency(self):
        in_scope_metrics = list(self.parent_metrics(self.node, self.manifest))

        to_return = []
        for metric in in_scope_metrics:
            if metric.calculation_method == "derived" and metric.name not in to_return:
                to_return.append(metric.name)

        return to_return

    def derived_metric_dependency_depth(self):
        metric_depth_count = 1
        to_return = list(self.reverse_dag_parsing(self.node, self.manifest, metric_depth_count))

        return to_return
