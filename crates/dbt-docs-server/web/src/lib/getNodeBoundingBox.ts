import { DagNodeData } from '../components/LineageV2/DagNode';

export const MAX_NODE_WIDTH = 360;

const RESOURCE_ICON_WIDTH = 44;
const NODE_PADDING = 16;
const RESOURCE_ICON_X_OFFSET = RESOURCE_ICON_WIDTH + NODE_PADDING;

export const NODE_HEIGHT = 82;
// NOTE: make sure this matches `NODE_HEIGHT` since Tailwind doesn't support dynamic values!
export const NODE_HEIGHT_CLASS = 'min-h-[82px]';

const LAYER_CHIP_OFFSET = 8;

export const getNodeBoundingBox = (node: DagNodeData, grain: string = 'table') => {
  const { name, resourceType } = node;
  const labelIcon = 50;

  const labelBox = getNodeSize(name, 'primary');

  const resourceTypeBox = getNodeSize(node.resourceType.toUpperCase(), 'primary');

  const minWidth = labelBox.width;

  const labelRowWidth = labelBox.width;
  const resourceTypeWidth = grain === 'column' ? resourceTypeBox.width + 28 : 0;
  const cllAddedHeight = grain === 'column' && resourceType !== 'error' ? 32 : 0;

  const nodeWidth = Math.max(
    minWidth,
    Math.min(
      MAX_NODE_WIDTH,
      RESOURCE_ICON_X_OFFSET +
        Math.max(labelRowWidth + (labelIcon ? 50 : 0), resourceTypeWidth) +
        2 * NODE_PADDING,
    ),
  );
  const nodeHeight = NODE_HEIGHT + LAYER_CHIP_OFFSET + cllAddedHeight;

  return {
    nodeWidth,
    nodeHeight,
    layoutWidth: nodeWidth,
    layoutHeight: nodeHeight,
  };
};

// Sizing is required for Dagre layout calculations
const DEFAULT_TEXT_WIDTH = 80;
const PRIMARY_LABEL_HEIGHT = 14;

const SECONDARY_LABEL_HEIGHT = 13;

// 53.76 - 21.23
const Y_PADDING = 33;

const fontFamily = getComputedStyle(document.body)
  .getPropertyValue('font-family')
  .trim();

const PRIMARY_LABEL_FONT = `400 ${PRIMARY_LABEL_HEIGHT}px ${fontFamily}`;
const SECONDARY_LABEL_FONT = `400 ${SECONDARY_LABEL_HEIGHT}px ${fontFamily}`;

const canvas = document.createElement('canvas');

// This can be memoized
function getTextBoundingBox(text: string, font: CanvasTextDrawingStyles['font']) {
  const context = canvas.getContext('2d');
  // In older browsers, we may have to use consistently sized Nodes in the DAG
  if (!context) return { width: DEFAULT_TEXT_WIDTH, height: PRIMARY_LABEL_HEIGHT };
  context.font = font;
  const { width } = context.measureText(text);
  return { width: width, height: PRIMARY_LABEL_HEIGHT };
}

type TextType = 'primary' | 'secondary';

const fontMap: Record<TextType, string> = {
  primary: PRIMARY_LABEL_FONT,
  secondary: SECONDARY_LABEL_FONT,
};

export const getNodeSize = (
  text: string | undefined | null,
  type: 'primary' | 'secondary',
) => {
  if (!text) return { width: 0, height: 0 };
  const isPrimary = type === 'primary';
  const { width, height } = getTextBoundingBox(text, fontMap[type]);
  return {
    width,
    height: (isPrimary ? Y_PADDING : 0) + height,
  };
};
