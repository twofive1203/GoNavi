export interface TableBodyBottomPaddingOptions {
  hasHorizontalOverflow: boolean;
  floatingScrollbarHeight: number;
  floatingScrollbarGap: number;
}

const MIN_SCROLLBAR_CLEARANCE = 8;
const FLOATING_SCROLLBAR_VISUAL_EXTRA = 4;

export const calculateTableBodyBottomPadding = ({
  hasHorizontalOverflow,
  floatingScrollbarHeight,
  floatingScrollbarGap,
}: TableBodyBottomPaddingOptions): number => {
  if (!hasHorizontalOverflow) {
    return 0;
  }

  const safeScrollbarHeight = Math.max(0, Math.ceil(floatingScrollbarHeight));
  const safeScrollbarGap = Math.max(0, Math.ceil(floatingScrollbarGap));

  return safeScrollbarHeight + FLOATING_SCROLLBAR_VISUAL_EXTRA + safeScrollbarGap + MIN_SCROLLBAR_CLEARANCE;
};
