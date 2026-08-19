import { Icon, RyeconSpinner } from '@dbt-labs/sourdough';

export const Spinner = () => {
  return <Icon ryecon={RyeconSpinner} className="motion-safe:animate-spin" />;
};
