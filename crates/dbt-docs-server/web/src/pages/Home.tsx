import type { ComponentProps } from 'react';

import { CatalogHome } from '../components/CatalogHome';

export default function Home(props: ComponentProps<typeof CatalogHome>) {
  return <CatalogHome {...props} />;
}
