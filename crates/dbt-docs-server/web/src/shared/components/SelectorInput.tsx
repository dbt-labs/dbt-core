import { twMerge } from 'tailwind-merge';

import { RyeconInfoOutline, RyeconMagnifyingGlass } from '@dbt-labs/sourdough';

import { Input, type InputProps } from '../../components/ui/Input';

export interface SelectorInputProps extends InputProps {
  className?: string;
  inputClassName?: string;
  autoFocus?: boolean;
  placeholder?: string;
  endIconEnabled?: boolean;
}

export const SelectorInput = ({
  className,
  inputClassName,
  placeholder = 'Search with selectors (e.g. model_name+) or press Enter to view full lineage',
  endIconEnabled = true,
  onChange,
  ...props
}: SelectorInputProps) => {
  return (
    <Input
      label="Search using selectors"
      labelIsHidden
      spellCheck={false}
      isEdit
      startIcon={{ ryecon: RyeconMagnifyingGlass }}
      {...(endIconEnabled
        ? {
            endIcon: {
              ryecon: RyeconInfoOutline,
              onClick: () => {
                window.open('https://docs.getdbt.com/reference/node-selection/syntax');
              },
            },
          }
        : null)}
      placeholder={placeholder}
      className={twMerge('mr-1 flex-1', className)}
      inputClassName={inputClassName}
      onChange={onChange}
      {...props}
    />
  );
};
