import {
  ColumnDef,
  DefaultTData,
  Pagination,
  PaginationFooterProps,
  Table,
  TableProps,
} from '@dbt-labs/sourdough';

const ROW_HEIGHT_PX = 52;

const calcTableHeight = (
  pageSize: number,
  count: number,
  includeSpaceForLoadMore = false,
) => {
  if (count === 0) {
    return ROW_HEIGHT_PX * 4;
  }
  if (count < pageSize) {
    return ROW_HEIGHT_PX * (count + 2);
  }
  return pageSize * ROW_HEIGHT_PX + ROW_HEIGHT_PX * (includeSpaceForLoadMore ? 2 : 1);
};

type PaginatedRowData = DefaultTData & {
  href?: string;
};

export type PaginationType = 'loadMore' | 'pageBased';

interface BasePaginatedTableProps<
  TData extends PaginatedRowData,
> extends TableProps<TData> {
  /** The list of all results */
  data: TData[];
  /** If true, we are loading the initial set of data */
  isLoading: boolean;
  /** An estimate or exact count of results that could be loaded in the future */
  maxResultCount: number | undefined;
  /** Number of results per page */
  pageSize: number;
  /** ColumnDefs to display in the table's heading */
  columns: ColumnDef<TData>[];
}

interface PaginatedTableProps<
  TData extends PaginatedRowData,
> extends BasePaginatedTableProps<TData> {
  /** Type of pagination to use */
  paginationType?: PaginationType;
  /** Parameters for controlling load more pagination */
  pagination?: PaginationFooterProps;
  /** Current page number (0-based) for page-based pagination */
  currentPage?: number;
  /** Function to set the current page for page-based pagination */
  setCurrentPage?: (page: number) => void;
}

export const PaginatedTable = <TData extends object>({
  paginationType = 'loadMore',
  pagination,
  currentPage,
  setCurrentPage,
  ...props
}: PaginatedTableProps<TData>) => {
  if (paginationType === 'loadMore') {
    if (!pagination) {
      throw new Error('Pagination props are required for loadMore pagination type');
    }
    return <LoadMorePaginatedTable pagination={pagination} {...props} />;
  }
  if (currentPage === undefined || !setCurrentPage) {
    throw new Error(
      'currentPage and setCurrentPage are required for pageBased pagination type',
    );
  }
  return (
    <PageBasedPaginatedTable
      currentPage={currentPage}
      setCurrentPage={setCurrentPage}
      {...props}
    />
  );
};

interface LoadMorePaginatedTableProps<
  TData extends PaginatedRowData,
> extends BasePaginatedTableProps<TData> {
  /** Parameters for controlling pagination */
  pagination: PaginationFooterProps;
}

const LoadMorePaginatedTable = <TData extends object>({
  pagination,
  data,
  isLoading,
  maxResultCount,
  pageSize,
  columns,
  ...tableProps
}: LoadMorePaginatedTableProps<TData>) => {
  return (
    <Table
      {...tableProps}
      isLoading={isLoading}
      data={data}
      height={tableProps.height ?? calcTableHeight(pageSize, data.length, true)}
      isWindowed={true}
      loadingRowCount={pageSize}
      columns={columns}
      rowHeight={ROW_HEIGHT_PX}
      paginationProps={{
        pageDisplayLabel:
          maxResultCount != null
            ? `Loaded ${data.length} of ${maxResultCount}`
            : undefined,
        selectedPageCount: {
          label: `${pageSize} Rows`,
          value: pageSize,
        },
        ...pagination,
      }}
    />
  );
};

interface PageBasedPaginatedTableProps<
  TData extends PaginatedRowData,
> extends BasePaginatedTableProps<TData> {
  /** Current page number (0-based) */
  currentPage: number;
  /** Function to set the current page */
  setCurrentPage: (page: number) => void;
}

const PageBasedPaginatedTable = <TData extends object>({
  data,
  isLoading,
  maxResultCount,
  pageSize,
  columns,
  currentPage,
  setCurrentPage,
  ...tableProps
}: PageBasedPaginatedTableProps<TData>) => {
  return (
    <>
      <Table
        {...tableProps}
        isLoading={isLoading}
        data={data}
        height={tableProps.height ?? calcTableHeight(pageSize, data.length)}
        isWindowed={true}
        loadingRowCount={pageSize}
        columns={columns}
        rowHeight={ROW_HEIGHT_PX}
      />
      <Pagination
        onPageChange={setCurrentPage}
        totalRows={maxResultCount ?? 0}
        currentPage={currentPage}
        rowsPerPage={pageSize}
      />
    </>
  );
};
