import typing as t
from difflib import unified_diff

T = t.TypeVar("T")


class SimpleComparator(t.Generic[T]):
    """A base class for comparing two objects of type T."""

    def __init__(
        self,
        left: T,
        right: T,
        operator: t.Callable[[T, T], bool] = lambda x, y: x == y,
        left_name: str = "current",
        right_name: str = "previous",
    ) -> None:
        """Create a new SimpleComparator."""
        self.operator = operator
        self.left = left
        self.right = right
        self.left_name = left_name
        self.right_name = right_name
        self.chained_comparisons = [self]

    def compare_operands(self) -> bool:
        """Return a boolean comparison of the two objects."""
        return self.operator(self.left, self.right)

    def diff_operands(self) -> str:
        """Return a str diff of the two objects.

        This method should be overridden to provide more detailed output.
        """
        return "" if self.compare_operands() else f"{self.left_name} != {self.right_name}"

    def diff(self) -> str:
        """Return the diff."""
        return "\n".join(
            self.chained_comparisons[i].diff_operands()
            for i in range(len(self.chained_comparisons))
            if not self.chained_comparisons[i].compare_operands()
        )

    def __str__(self) -> str:
        """Return a string representation of the diff."""
        return self.diff()

    def __bool__(self) -> bool:
        """Return True if the two objects are equal."""
        return all(op.compare_operands() for op in self.chained_comparisons)

    def __or__(self, other: "SimpleComparator[T]") -> "SimpleComparator":
        """Add the current comparator to the list of operations."""
        self.chained_comparisons.extend(other.chained_comparisons)
        return self

    def __ror__(self, other: t.Union["SimpleComparator[T]", bool]) -> "SimpleComparator":
        """Add the other comparator to the list of operations."""
        if isinstance(other, bool):
            self.chained_comparisons.append(
                SimpleComparator(other, True, left_name="previous", right_name="current")
            )
            return self
        else:
            other.chained_comparisons.extend(self.chained_comparisons)
            return other

    def __repr__(self) -> str:
        return " | ".join(
            f"Diff({op.left_name}, {op.right_name})" for op in self.chained_comparisons
        )


class TextComparator(SimpleComparator[str]):
    def diff_operands(self) -> str:
        """Return a unified diff of the two strings."""
        return "\n".join(
            unified_diff(
                self.left.splitlines(keepends=True),
                self.right.splitlines(keepends=True),
                fromfile=self.left_name,
                tofile=self.right_name,
                lineterm="",
            )
        )


class DictComparator(SimpleComparator[t.Dict[str, t.Any]]):
    def diff_operands(self) -> str:
        """Return a unified diff of the two dicts."""
        return "\n".join(
            unified_diff(
                sorted(f"{k}: {v}" for k, v in self.left.items()),
                sorted(f"{k}: {v}" for k, v in self.right.items()),
                fromfile=self.left_name,
                tofile=self.right_name,
                lineterm="",
            )
        )


class SetComparator(SimpleComparator[t.Set[str]]):
    def diff_operands(self) -> str:
        """Return a unified diff of the two dicts."""
        return f"Present in {self.left_name} but not {self.right_name}: {self.left - self.right}"
