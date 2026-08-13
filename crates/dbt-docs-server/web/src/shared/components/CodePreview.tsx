import { FC, useState } from 'react';

import { CodeSnippet, FloatingTab, FloatingTabs } from '@dbt-labs/sourdough';

type CodePreviewParams = {
  source: string;
  compiled: string | undefined;
  classNames?: string;
};

type CodeType = 'source' | 'compiled';

// large SQL files can cause browser crashes when syntax highlighting is applied
const SYNTAX_HIGHLIGHTING_THRESHOLD = 256 * 1024; // 256 KB

export const CodePreview: FC<CodePreviewParams> = (params) => {
  const [activeTab, setActiveTab] = useState<CodeType>('source');
  const fileContents = activeTab === 'source' ? params.source : params.compiled || '';
  const isLargeFile = fileContents.length > SYNTAX_HIGHLIGHTING_THRESHOLD;

  return (
    <div className={params.classNames}>
      <div className="mb-4 overflow-x-auto overflow-y-hidden">
        <FloatingTabs>
          <FloatingTab
            text="Source"
            id="source"
            isActive={activeTab === 'source'}
            onClick={() => setActiveTab('source')}
            trackingId="explorer-source-code"
          />
          {params.compiled ? (
            <FloatingTab
              text="Compiled"
              id="compiled"
              isActive={activeTab === 'compiled'}
              onClick={() => setActiveTab('compiled')}
              trackingId="explorer-compiled-code"
            />
          ) : null}
        </FloatingTabs>
      </div>
      {isLargeFile && (
        <div className="mb-3 rounded-md border border-borderWarning bg-bgWarningMuted p-3 text-sm text-fgWarning">
          This file is large ({(fileContents.length / 1024).toFixed(0)} KB) and may
          cause performance issues. Syntax highlighting has been disabled.
        </div>
      )}
      <CodeSnippet
        language={isLargeFile ? undefined : 'sql'}
        code={fileContents}
        includeCopyButton
      />
    </div>
  );
};
