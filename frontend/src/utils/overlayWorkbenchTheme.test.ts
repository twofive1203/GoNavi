import { strict as assert } from 'node:assert';

import { buildOverlayWorkbenchTheme } from './overlayWorkbenchTheme';

const darkTheme = buildOverlayWorkbenchTheme(true);
assert.equal(darkTheme.isDark, true);
assert.match(darkTheme.shellBg, /rgba\(15, 15, 17,/);
assert.match(darkTheme.sectionBg, /rgba\(255,?\s*255,?\s*255,?\s*0\.03\)/);
assert.equal(darkTheme.iconColor, '#ffd666');

const lightTheme = buildOverlayWorkbenchTheme(false);
assert.equal(lightTheme.isDark, false);
assert.match(lightTheme.shellBg, /rgba\(255,255,255,0\.98\)/);
assert.match(lightTheme.sectionBg, /rgba\(255,?\s*255,?\s*255,?\s*0\.84\)/);
assert.equal(lightTheme.iconColor, '#1677ff');

console.log('overlayWorkbenchTheme tests passed');
