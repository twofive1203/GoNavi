import { strict as assert } from 'node:assert';

import { buildRedisWorkbenchTheme } from './redisViewerWorkbenchTheme';

const darkTheme = buildRedisWorkbenchTheme({
  darkMode: true,
  opacity: 0.72,
  blur: 14,
});

assert.equal(darkTheme.isDark, true);
assert.match(darkTheme.panelBg, /^rgba\(/);
assert.match(darkTheme.toolbarPrimaryBg, /^linear-gradient\(/);
assert.notEqual(darkTheme.actionDangerBg, darkTheme.actionSecondaryBg);
assert.notEqual(darkTheme.treeSelectedBg, darkTheme.treeHoverBg);
assert.match(darkTheme.appBg, /rgba\(15, 15, 17,/);
assert.match(darkTheme.panelBg, /rgba\(24, 24, 28,/);
assert.match(darkTheme.panelBgStrong, /rgba\(31, 31, 36,/);
assert.equal(darkTheme.backdropFilter, 'blur(14px)');

const lightTheme = buildRedisWorkbenchTheme({
  darkMode: false,
  opacity: 1,
  blur: 0,
});

assert.equal(lightTheme.isDark, false);
assert.match(lightTheme.panelBg, /^rgba\(/);
assert.match(lightTheme.contentEmptyBg, /^linear-gradient\(/);
assert.notEqual(lightTheme.textPrimary, lightTheme.textSecondary);
assert.notEqual(lightTheme.statusTagBg, lightTheme.statusTagMutedBg);
assert.equal(lightTheme.backdropFilter, 'none');

console.log('redisViewerWorkbenchTheme tests passed');
