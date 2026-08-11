import { type FC, type JSX } from 'react';

import {
  Link,
  RyeconStatusError,
  RyeconStatusSuccess,
  RyeconStatusWarning,
  SizeType,
} from '@dbt-labs/sourdough';

import { toTitleCase } from '../util/string';
import { TrustSignalMessage, TrustState } from '../util/trustSignals';
import { trustIconMap } from './constants';

export const trustStateTraits: Record<
  TrustState,
  { icon: JSX.Element; textColor: string }
> = {
  healthy: {
    icon: <RyeconStatusSuccess size="sm" />,
    textColor: 'text-fgSuccess',
  },
  caution: {
    icon: <RyeconStatusWarning size="sm" />,
    textColor: 'text-fgWarning',
  },
  degraded: {
    icon: <RyeconStatusError size="sm" />,
    textColor: 'text-fgDanger',
  },
  unknown: {
    icon: <RyeconStatusError size="sm" />,
    textColor: 'text-fgDecorative',
  },
};

type TrustSignalDescriptionProps = {
  trustState: TrustState;
  messages: TrustSignalMessage[];
  size?: SizeType;
};

export const TrustSignalDescription: FC<TrustSignalDescriptionProps> = ({
  trustState,
  messages,
  size = 'md',
}) => {
  const trustStateTitleCase = toTitleCase(trustState);
  const icon = trustIconMap(size)[trustState];

  if (!messages) {
    return null;
  }
  const order = ['unknown', 'degraded', 'caution', 'healthy'];
  const sortedMessages = [...messages].sort((a, b) => {
    const severityComparison = order.indexOf(a.type) - order.indexOf(b.type);
    if (severityComparison !== 0) {
      return severityComparison;
    }
    return a.importance - b.importance;
  });

  return (
    <div className="z-50">
      <div className="flex items-center">
        <span>{icon}</span>
        <span>
          <h3
            className={`ml-2 text-base font-semibold ${trustStateTraits[trustState].textColor}`}
          >
            {trustStateTitleCase}
          </h3>
        </span>
      </div>
      <div>
        <p className="font-caption mt-0.5">{`${trustStateTitleCase} means that this resource consists of the following:`}</p>
      </div>
      <div className="w-90 mt-2">
        <ul className="space-y-1">
          {sortedMessages.map((message: TrustSignalMessage, index) => (
            <li key={`trust-signal-${index}`} className="flex items-center">
              {trustStateTraits[message.type as TrustState].icon}
              <p className="font-body ml-2">
                {message.link ? (
                  <Link
                    isInternal
                    to={{ pathname: message.link.to }}
                    state={{ ...message.link.state }}
                  >
                    {message.text}
                  </Link>
                ) : (
                  message.text
                )}
              </p>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
};
