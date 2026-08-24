import { useCallback, useState } from 'react';

import { RyeconCheckmark, RyeconCopy, Sizes } from '@dbt-labs/sourdough';

import { Button } from '../../../components/ui/Button';
import { Code } from '../../../components/ui/Code';

/** Inline `run: <code>` snippet with an icon-only copy button — the CTA
 *  pattern the Notion handoff specifies for the column-level-lineage row
 *  (run `dbt login` and copy). */

interface Props {
  command: string;
  className?: string;
}

export function CopyCommandSnippet({ command, className }: Props) {
  const [copied, setCopied] = useState(false);

  const handleCopy = useCallback(() => {
    navigator.clipboard.writeText(command);
    setCopied(true);
    setTimeout(() => setCopied(false), 1500);
  }, [command]);

  return (
    <div className={`inline-flex shrink-0 items-center gap-2 ${className ?? ''}`}>
      <span className="text-xs text-fgDecorative">run:</span>
      <Code>{command}</Code>
      <Button
        variant="ghost"
        size={Sizes.xs}
        ariaLabel={copied ? 'Copied to clipboard' : `Copy ${command}`}
        ryecon={copied ? RyeconCheckmark : RyeconCopy}
        onClick={handleCopy}
        testId="upgrade-copy-command"
      />
      <span aria-live="polite" className="sr-only">
        {copied && `Copied ${command} to clipboard`}
      </span>
    </div>
  );
}
