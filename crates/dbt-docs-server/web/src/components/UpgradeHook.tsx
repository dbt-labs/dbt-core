import { useState } from "react";
import {
  Button,
  Icon,
  RyeconArrowRight,
  RyeconCaretDown,
  RyeconColorDbt,
  RyeconLinkExternal,
} from "@dbt-labs/sourdough";

/** "Get more from dbt" panel — status-style card surfacing the v0 hooks
 *  (CLL, Cost insights, Cross-project Mesh). Mirrors the Catalog spec at
 *  Figma OMejLgivWDSbjaEpEdWmf6 node 18484:44009.
 *
 *  Each row: status dot · title · description (with optional doc link) ·
 *  3×2 mono-dot sigil on the right. Status dot is brand-green when the
 *  capability is on, fgDecorative when off.
 *
 *  IMPORTANT: per Elias's Slack clarification, Query mode is local-only.
 *  No copy in this panel mentions Query — Query is free and stays free. */

interface RowSpec {
  key: "cll" | "cost" | "mesh";
  on: boolean;
  title: string;
  /** Body text. Plain string OR a tuple of fragments where strings render
   *  as text and `{ code }` renders as a monospace snippet. */
  description: Array<string | { code: string }>;
  linkLabel?: string;
  linkHref?: string;
  /** Cells filled in the 3×2 sigil grid. (1,1) is top-left. */
  pattern: Array<[1 | 2 | 3, 1 | 2]>;
}

interface PanelProps {
  /** From `Capabilities.has_column_lineage`. Off rows are always off for v0. */
  hasColumnLineage: boolean;
  className?: string;
}

export function UpgradeHookPanel({ hasColumnLineage, className }: PanelProps) {
  const rows: RowSpec[] = [
    {
      key: "cll",
      on: hasColumnLineage,
      title: hasColumnLineage ? "Column lineage is on" : "Column lineage is off",
      description: hasColumnLineage
        ? [
            "Every model has column-level upstream and downstream edges. Open any model to see the graph.",
          ]
        : [
            "Run with ",
            { code: "--static-analysis strict" },
            " to populate column-level upstream and downstream edges.",
          ],
      pattern: [
        [1, 1],
        [2, 1],
        [3, 1],
        [1, 2],
      ],
    },
    {
      key: "cost",
      on: false,
      title: "Cost insights is off",
      description: [
        "Understand your spend and savings. Run ",
        { code: "dbt state" },
        " to start. ",
      ],
      linkLabel: "dbt state docs",
      linkHref:
        "https://docs.getdbt.com/reference/node-selection/state-comparison-caveats",
      pattern: [
        [1, 1],
        [3, 1],
        [1, 2],
        [2, 2],
      ],
    },
    {
      key: "mesh",
      on: false,
      title: "Cross-project mesh is off",
      description: ["Unlock more with cross-project views in dbt docs. "],
      linkLabel: "dbt cross-project mesh docs",
      linkHref: "https://docs.getdbt.com/docs/mesh/about-mesh?version=1.12",
      pattern: [
        [3, 1],
        [1, 2],
        [2, 2],
        [3, 2],
      ],
    },
  ];

  return (
    <section className={`upgrade-panel ${className ?? ""}`} aria-label="Get more from dbt">
      <div className="upgrade-panel__bg" aria-hidden />
      <h2 className="upgrade-panel__title">Get more from dbt</h2>
      <ul className="upgrade-panel__rows">
        {rows.map((r) => (
          <UpgradeRow key={r.key} row={r} />
        ))}
        <li className="upgrade-row upgrade-row--tips">
          <div className="upgrade-row__body upgrade-row__body--tips">
            <span className="upgrade-panel__tips-label">TRY</span>
            <span className="upgrade-panel__tips-item">
              <code className="upgrade-row__code">dbt run</code>
              <span className="upgrade-panel__tips-hint">regenerate metadata</span>
            </span>
            <span className="upgrade-panel__tips-item">
              <code className="upgrade-row__code">dbt test</code>
              <span className="upgrade-panel__tips-hint">populate test results</span>
            </span>
          </div>
          <Button
            type="secondary"
            size={"sm" as never}
            text="Need Enterprise? Contact sales"
            onClick={() => {
              window.open("https://www.getdbt.com/contact-sales", "_blank");
            }}
          />
        </li>
      </ul>
    </section>
  );
}

function UpgradeRow({ row }: { row: RowSpec }) {
  return (
    <li className="upgrade-row">
      <div className="upgrade-row__lead">
        <span
          className={`upgrade-row__dot ${row.on ? "is-on" : "is-off"}`}
          aria-hidden
        />
        <div className="upgrade-row__body">
          <div className="upgrade-row__title">{row.title}</div>
          <p className="upgrade-row__desc">
            {row.description.map((fragment, i) =>
              typeof fragment === "string" ? (
                <span key={i}>{fragment}</span>
              ) : (
                <code key={i} className="upgrade-row__code">
                  {fragment.code}
                </code>
              ),
            )}
            {row.linkLabel && row.linkHref && (
              <a
                className="upgrade-row__link"
                href={row.linkHref}
                target="_blank"
                rel="noopener noreferrer"
              >
                {row.linkLabel}
              </a>
            )}
          </p>
        </div>
      </div>
      {/* On rows show the dot-sigil glyph; off rows show an "Upgrade" CTA
          (per Figma OMejLgivWDSbjaEpEdWmf6 #18487:34217). */}
      {row.on ? (
        <Sigil pattern={row.pattern} />
      ) : (
        <Button
          type="secondary"
          size={"sm" as never}
          text="Upgrade"
          onClick={() => {
            if (row.linkHref) window.open(row.linkHref, "_blank");
          }}
        />
      )}
    </li>
  );
}

/** Tiny 3×2 sigil glyph (per Figma: 8px squares, 5px x-gap, 6px y-gap). */
function Sigil({ pattern }: { pattern: RowSpec["pattern"] }) {
  const set = new Set(pattern.map(([c, r]) => `${c}:${r}`));
  const cells: Array<[1 | 2 | 3, 1 | 2]> = [
    [1, 1],
    [2, 1],
    [3, 1],
    [1, 2],
    [2, 2],
    [3, 2],
  ];
  return (
    <div className="upgrade-row__sigil" aria-hidden>
      {cells.map(([c, r]) => (
        <span
          key={`${c}:${r}`}
          className={`upgrade-row__sigil-cell ${
            set.has(`${c}:${r}`) ? "is-on" : ""
          }`}
          style={{ gridColumn: c, gridRow: r }}
        />
      ))}
    </div>
  );
}

/** Inline rail-footer affordance. Stays — different surface, different job
 *  from the dense status panel above. */
interface InlineProps {
  className?: string;
}

export function UpgradeHookInline({ className }: InlineProps) {
  const [open, setOpen] = useState(false);
  return (
    <div className={`upgrade-hook-inline-wrap ${className ?? ""}`}>
      <button
        type="button"
        className="upgrade-hook upgrade-hook--inline"
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
        title="Connect dbt platform"
      >
        <Icon ryecon={RyeconColorDbt} size="xs" alt="" />
        <span className="upgrade-hook__title-inline">Connect dbt platform</span>
        <span
          className={`upgrade-hook__chev ${open ? "is-open" : ""}`}
          aria-hidden
        >
          <Icon ryecon={open ? RyeconCaretDown : RyeconArrowRight} size="xs" alt="" />
        </span>
      </button>
      {open && (
        <div className="upgrade-hook-inline-detail">
          <p className="upgrade-hook-inline-detail__line">
            Run <code className="upgrade-row__code">dbt login</code> from the CLI
          </p>
          <p className="upgrade-hook-inline-detail__line">
            or{" "}
            <a
              className="upgrade-row__link"
              href="https://cloud.getdbt.com/signup"
              target="_blank"
              rel="noopener noreferrer"
            >
              sign in to dbt platform
              <Icon ryecon={RyeconLinkExternal} size="xs" alt="" />
            </a>
          </p>
        </div>
      )}
    </div>
  );
}
