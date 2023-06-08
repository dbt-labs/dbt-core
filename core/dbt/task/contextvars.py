from contextvars import ContextVar

# This is a place to hold common contextvars used in tasks so that we can
# avoid circular imports.

cv_project_root: ContextVar = ContextVar("project_root")
