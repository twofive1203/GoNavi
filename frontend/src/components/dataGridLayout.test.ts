import { strict as assert } from 'node:assert';

import { calculateTableBodyBottomPadding } from './dataGridLayout';

assert.equal(
  calculateTableBodyBottomPadding({
    hasHorizontalOverflow: false,
    floatingScrollbarHeight: 10,
    floatingScrollbarGap: 6,
  }),
  0,
  '无横向滚动条时不应增加底部间距'
);

assert.equal(
  calculateTableBodyBottomPadding({
    hasHorizontalOverflow: true,
    floatingScrollbarHeight: 10,
    floatingScrollbarGap: 6,
  }),
  28,
  '默认悬浮滚动条应预留滚动条高度、间距和额外安全区'
);

assert.equal(
  calculateTableBodyBottomPadding({
    hasHorizontalOverflow: true,
    floatingScrollbarHeight: 14,
    floatingScrollbarGap: 4,
  }),
  30,
  '较粗滚动条场景下应同步放大底部安全区'
);

console.log('dataGridLayout tests passed');
