import { type ComponentPropsWithRef } from 'react';

import { Icon, type Ryecon } from '@dbt-labs/sourdough';

import { cn } from '../../lib/utils';

interface InputIconProps {
  ryecon: Ryecon;
  onClick?: () => void;
  className?: string;
}

export interface InputProps extends Omit<ComponentPropsWithRef<'input'>, 'className'> {
  label?: string;
  labelIsHidden?: boolean;
  startIcon?: InputIconProps;
  endIcon?: InputIconProps;
  className?: string;
  inputClassName?: string;
  testId?: string;
  /** Accepted for drop-in parity with sourdough's Input; we don't have a non-edit display mode. */
  isEdit?: boolean;
}

function InputIcon({ ryecon, onClick, className }: InputIconProps) {
  const icon = <Icon ryecon={ryecon} size="xs" />;
  if (!onClick) return <span className={className}>{icon}</span>;
  return (
    <button type="button" onClick={onClick} className={className}>
      {icon}
    </button>
  );
}

export function Input({
  label,
  labelIsHidden,
  startIcon,
  endIcon,
  className,
  inputClassName,
  testId,
  id,
  name,
  isEdit: _isEdit,
  ...props
}: InputProps) {
  const inputId = id ?? name;
  return (
    <div className={cn('relative flex items-center', className)}>
      {label && (
        <label htmlFor={inputId} className={labelIsHidden ? 'sr-only' : 'mb-1 block'}>
          {label}
        </label>
      )}
      {startIcon && (
        <InputIcon
          {...startIcon}
          className="absolute left-2 flex items-center text-fgDecorative"
        />
      )}
      <input
        id={inputId}
        name={name}
        data-testid={testId}
        className={cn(
          'flex h-9 w-full rounded-md border border-borderMain bg-bgMain px-3 py-1 text-sm text-fgMain shadow-sm transition-colors placeholder:text-fgDecorative focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-bgBrand disabled:cursor-not-allowed disabled:opacity-50',
          startIcon && 'pl-8',
          endIcon && 'pr-8',
          inputClassName,
        )}
        {...props}
      />
      {endIcon && (
        <InputIcon
          {...endIcon}
          className="absolute right-2 flex items-center text-fgDecorative"
        />
      )}
    </div>
  );
}
