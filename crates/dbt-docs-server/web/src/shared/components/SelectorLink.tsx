import { Button, Link } from '@dbt-labs/sourdough';

interface Props {
  selector: string;
  label: string;
}

export function SelectorLink({ selector, label }: Props) {
  return (
    <Link isInternal to={`?select=${encodeURIComponent(selector)}`}>
      <Button type="secondary" className="mt-6" text={label} />
    </Link>
  );
}
