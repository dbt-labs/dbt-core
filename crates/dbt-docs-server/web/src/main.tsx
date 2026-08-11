import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { BrowserRouter } from 'react-router-dom';
import { QueryClientProvider } from '@tanstack/react-query';

import App from './App';
import { queryClient } from './queryClient';
import { LinkPrefixProvider, MetadataDataProvider, wrapDataSource } from './shared';
import { createRestDataSource } from './shared/data-sources/rest';

import './index.css';

const root = document.getElementById('root');
if (!root) throw new Error('missing #root element');

const dataSource = wrapDataSource(createRestDataSource(), queryClient);

createRoot(root).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <LinkPrefixProvider prefix="/">
          <MetadataDataProvider source={dataSource}>
            <App />
          </MetadataDataProvider>
        </LinkPrefixProvider>
      </BrowserRouter>
    </QueryClientProvider>
  </StrictMode>,
);
