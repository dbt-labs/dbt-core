import type { Ryecon } from '@dbt-labs/sourdough';
import {
  RyeconBook,
  RyeconClipboardSuccess,
  RyeconFile,
  RyeconFileBlank,
} from '@dbt-labs/sourdough';

import type { FileTreeItemType } from '../components/ui/PaginatedFileTree';
import type { FileEntry } from '../shared';
import { RESOURCE_TYPE_RYECON } from './resourceType';

const EXTRA_RYECON: Record<string, Ryecon> = {
  unit_test: RyeconClipboardSuccess,
  doc: RyeconBook,
};

export function iconForResourceType(resourceType: string): Ryecon {
  return RESOURCE_TYPE_RYECON[resourceType] ?? EXTRA_RYECON[resourceType] ?? RyeconFile;
}

const YAML_PATH_RE = /\.ya?ml$/i;

export interface BuiltFileTree {
  items: FileTreeItemType[];
  /** Tree-id (full path) → resource unique_id. */
  pathToUniqueId: Map<string, string>;
  /** Total file rows (excludes folders). Used for the locate-pane summary. */
  fileCount: number;
}

/** Turn the flat FileEntry list into the shape sourdough's FileTree expects:
 *  one item per path segment, files marked `pathType: 'file'` and folders
 *  marked `pathType: 'directory'`. Each item's `id` is its full path so the
 *  component can derive depth + parent from path-splitting alone.
 *
 *  Everything is nested under a single synthetic root folder named `rootName`
 *  so sourdough's treeWalker (which has a bug when fed multiple roots) sees
 *  exactly one root child of `'root'`. */
export function buildFileTreeItems(
  files: FileEntry[],
  rootName: string,
): BuiltFileTree {
  const itemsByPath = new Map<string, FileTreeItemType>();
  const pathToUniqueId = new Map<string, string>();
  let fileCount = 0;

  itemsByPath.set(rootName, {
    id: rootName,
    parent: 'root',
    data: { pathType: 'directory' },
  });

  for (const file of files) {
    const segments = pathSegments(file);
    if (segments.length === 0) continue;
    fileCount += 1;

    for (let i = 0; i < segments.length; i += 1) {
      const path = `${rootName}/${segments.slice(0, i + 1).join('/')}`;
      const isLeaf = i === segments.length - 1;
      if (itemsByPath.has(path)) continue;

      const parent =
        i === 0 ? rootName : `${rootName}/${segments.slice(0, i).join('/')}`;

      if (isLeaf) {
        itemsByPath.set(path, {
          id: path,
          parent,
          data: {
            pathType: 'file',
            iconOverride: {
              ryecon: iconForResourceType(file.resourceType),
              label: file.resourceType,
            },
          },
        });
        pathToUniqueId.set(path, file.uniqueId);
      } else {
        const segment = segments[i];
        const isYamlDir = YAML_PATH_RE.test(segment);
        itemsByPath.set(path, {
          id: path,
          parent,
          data: {
            pathType: 'directory',
            ...(isYamlDir && {
              iconOverride: { ryecon: RyeconFileBlank, label: 'yaml' },
            }),
          },
        });
      }
    }
  }

  return {
    items: Array.from(itemsByPath.values()),
    pathToUniqueId,
    fileCount,
  };
}

/** Files whose `original_file_path` is a YAML doc (e.g. `models/_models.yml`)
 *  are turned into folders, with one leaf per resource defined inside that
 *  file. The leaf segment is the resource's own `name`, so two tests sharing
 *  `_models.yml` no longer collide on path-dedup. */
function pathSegments(file: FileEntry): string[] {
  if (!file.packageName || !file.originalFilePath) return [];
  const parts = file.originalFilePath.split('/').filter(Boolean);
  if (parts.length === 0) return [];
  const last = parts[parts.length - 1];
  if (YAML_PATH_RE.test(last) && file.name) {
    return [file.packageName, ...parts, file.name];
  }
  return [file.packageName, ...parts];
}
