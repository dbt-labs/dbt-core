import { Markdown } from '../components/Overview/Markdown';
import bundledOverview from '../components/Overview/overview.md?raw';
import { useProjectOverview } from '../shared';

/**
 * The project overview — the docs landing page, at parity with dbt Docs v1.
 *
 * Renders the winning `{% docs __overview__ %}` block when a package defines one,
 * and a bundled default otherwise. Note the filename is convention only: dbt
 * discovers docs blocks by walking `docs-paths` for `*.md`, so the block usually
 * lives in `models/overview.md` and nothing keys on what the file is called.
 *
 * v1's fallback was the `dbt` global project's own `doc.dbt.__overview__`, and
 * `overview.md` beside this file is a byte-copy of that block's body (dbt Core's
 * `dbt/include/global_project/docs/overview.md`), so an unconfigured project sees
 * exactly what it saw under v1.
 *
 * It is bundled rather than read from `doc.dbt.__overview__` at runtime for two
 * reasons: the fallback then survives an index that is missing or unreadable, and
 * the constant backing that row (`dbt-parser`'s `DEFAULT_OVERVIEW_CONTENTS`) is
 * pinned byte-for-byte to dbt Core's manifest by the conformance regression suite,
 * so it is not ours to evolve for this UI. The two texts differ only in soft
 * wrapping — the Rust constant joins lines that the markdown file breaks, which
 * renders identically.
 */
export default function Overview() {
  const { data, isPending, isError } = useProjectOverview();

  // An unreadable dbt.docs must not blank the landing page: the built-in overview
  // is a correct answer, not a degraded one.
  const authored = !isError && data?.blockContents.trim() ? data.blockContents : null;

  // Deliberately a spinner rather than the default while pending — flashing the
  // built-in copy and then swapping to the user's is worse than a brief wait.
  // `useSourceQuery` forces `isPending` false when the fetcher is absent, so a
  // source without `fetchOverview` falls straight through to the default.
  if (isPending) return <div className="main-inner muted">Loading…</div>;

  return (
    <div className="flex max-w-[768px] flex-col gap-3 px-8 pb-12 pt-6">
      <Markdown>{authored ?? bundledOverview}</Markdown>
    </div>
  );
}
