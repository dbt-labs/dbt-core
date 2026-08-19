import { FC } from 'react';

import {
  Badge,
  BadgeType,
  Ryecon,
  RyeconDuotoneStale,
  RyeconDuotoneUnknown,
  RyeconStatusError,
  RyeconStatusReused,
  RyeconStatusSkipped,
  RyeconStatusSuccess,
  RyeconStatusWarning,
  Tooltip,
} from '@dbt-labs/sourdough';

import type { RunStatus } from '../typings/domain/executionInfo';
import type { FreshnessStatusValue, TestStatusValue } from '../typings/domain/status';
import { toTitleCase } from '../util/string';

type BadgeSpec = {
  type: BadgeType;
  ryecon?: Ryecon;
  decorative?: boolean;
};

const RUN: Record<RunStatus, BadgeSpec> = {
  success: { type: 'success', ryecon: RyeconStatusSuccess },
  error: { type: 'error', ryecon: RyeconStatusError },
  running: { type: 'default' },
  queued: { type: 'default' },
  skipped: { type: 'default', ryecon: RyeconStatusSkipped },
  reused: { type: 'teal', ryecon: RyeconStatusReused },
};

const TEST: Record<TestStatusValue, BadgeSpec> = {
  pass: { type: 'success', ryecon: RyeconStatusSuccess },
  fail: { type: 'error', ryecon: RyeconStatusError },
  warn: { type: 'warning', ryecon: RyeconStatusWarning },
  error: { type: 'error', ryecon: RyeconStatusError },
  skipped: { type: 'default', ryecon: RyeconStatusSkipped },
  unknown: { type: 'default', ryecon: RyeconDuotoneUnknown, decorative: true },
};

const FRESH: Record<FreshnessStatusValue, BadgeSpec> = {
  pass: { type: 'success', ryecon: RyeconStatusSuccess },
  warn: { type: 'warning', ryecon: RyeconStatusWarning },
  error: { type: 'error', ryecon: RyeconStatusError },
  outdated: { type: 'default', ryecon: RyeconDuotoneStale, decorative: true },
  unconfigured: {
    type: 'default',
    ryecon: RyeconDuotoneStale,
    decorative: true,
  },
  skipped: { type: 'default', ryecon: RyeconStatusSkipped },
  unknown: { type: 'default', ryecon: RyeconDuotoneUnknown, decorative: true },
};

const NONE: BadgeSpec = { type: 'default' };
const RUNTIME_ERROR: BadgeSpec = { type: 'error', ryecon: RyeconStatusError };

// Union of every vocabulary. Typed as possibly-undefined because a caller can pass a
// status outside the requested kind at runtime (e.g. a test's lastRunStatus cast
// `as RunStatus`), so a lookup miss is expected and must not be treated as always-present.
const ANY_STATUS: Record<string, BadgeSpec | undefined> = { ...FRESH, ...TEST, ...RUN };

// Read a status by own-property only, so inherited keys ("toString", "constructor",
// "__proto__", …) resolve to undefined rather than to a prototype value.
const ownSpec = (
  map: Record<string, BadgeSpec | undefined>,
  status: string,
): BadgeSpec | undefined =>
  Object.prototype.hasOwnProperty.call(map, status) ? map[status] : undefined;

// Prefer the kind's own vocabulary, fall back to any known status, then a neutral badge —
// so a status outside the declared kind renders plainly instead of crashing (META-7840).
const resolveSpec = (
  vocabulary:
    | Record<RunStatus, BadgeSpec>
    | Record<TestStatusValue, BadgeSpec>
    | Record<FreshnessStatusValue, BadgeSpec>,
  status: string,
): BadgeSpec =>
  ownSpec(vocabulary as Record<string, BadgeSpec | undefined>, status) ??
  ownSpec(ANY_STATUS, status) ??
  NONE;

type NodeStatusIconBadgeProps = (
  | { kind: 'run'; status: RunStatus }
  | { kind: 'test'; status: TestStatusValue }
  | { kind: 'freshness'; status: FreshnessStatusValue }
  | { kind: 'none' }
  | { kind: 'runtimeError' }
) & {
  className?: string;
  size?: 'small' | 'standard';
  tooltip?: string;
};

const sizeToBadgeSize = (size: 'small' | 'standard'): 'sm' | 'lg' =>
  size === 'small' ? 'sm' : 'lg';

const resolve = (
  props: NodeStatusIconBadgeProps,
): { spec: BadgeSpec; text: string } => {
  switch (props.kind) {
    case 'run':
      return { spec: resolveSpec(RUN, props.status), text: toTitleCase(props.status) };
    case 'test':
      return { spec: resolveSpec(TEST, props.status), text: toTitleCase(props.status) };
    case 'freshness':
      return {
        spec: resolveSpec(FRESH, props.status),
        text: toTitleCase(props.status),
      };
    case 'runtimeError':
      return { spec: RUNTIME_ERROR, text: 'Runtime Error' };
    case 'none':
      return { spec: NONE, text: 'Not Available' };
  }
};

export const NodeStatusIconBadge: FC<NodeStatusIconBadgeProps> = (props) => {
  const { className, size, tooltip } = props;
  const { spec, text } = resolve(props);
  const decorativeClass = spec.decorative
    ? '[&_*:not(.ryecon-accent)]:!text-fgDecorative'
    : '';
  return (
    <Tooltip content={tooltip}>
      <Badge
        size={sizeToBadgeSize(size ?? 'standard')}
        type={spec.type}
        ryecon={spec.ryecon}
        className={`${decorativeClass} ${className ?? ''}`.trim()}
        text={text}
      />
    </Tooltip>
  );
};
