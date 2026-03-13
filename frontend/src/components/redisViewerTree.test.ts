import type { RedisKeyInfo } from '../types';
import {
  applyRenamedRedisKeyState,
  applyTreeNodeCheck,
  buildCheckedTreeNodeState,
  buildRedisKeyTree,
  isGroupFullyChecked,
} from './redisViewerTree';

const assert = (condition: unknown, message: string) => {
  if (!condition) {
    throw new Error(message);
  }
};

const assertEqual = (actual: unknown, expected: unknown, message: string) => {
  const actualText = JSON.stringify(actual);
  const expectedText = JSON.stringify(expected);
  if (actualText !== expectedText) {
    throw new Error(`${message}\nactual: ${actualText}\nexpected: ${expectedText}`);
  }
};

const sampleKeys: RedisKeyInfo[] = [
  { key: 'app:user:1', type: 'string', ttl: -1 },
  { key: 'app:user:2', type: 'string', ttl: -1 },
  { key: 'app:order:1', type: 'hash', ttl: 120 },
  { key: 'misc', type: 'set', ttl: -1 },
];

const tree = buildRedisKeyTree(sampleKeys, true);
const appGroup = tree.treeData.find((node) => node.key === 'group:app');
const userGroup = appGroup?.children?.find((node) => node.key === 'group:app:user');

assert(appGroup, '应生成 group:app 节点');
assert(userGroup, '应生成 group:app:user 节点');
assertEqual(
  appGroup?.descendantRawKeys,
  ['app:order:1', 'app:user:1', 'app:user:2'],
  'app 分组应收集全部后代 key'
);

const selectedAfterGroupCheck = applyTreeNodeCheck([], appGroup!, true);
assertEqual(
  selectedAfterGroupCheck,
  ['app:order:1', 'app:user:1', 'app:user:2'],
  '勾选分组应递归选中全部后代 key'
);

const checkedState = buildCheckedTreeNodeState(selectedAfterGroupCheck, tree);
assertEqual(
  checkedState.checked,
  ['key:app:order:1', 'group:app:order', 'key:app:user:1', 'key:app:user:2', 'group:app:user', 'group:app'],
  '全部后代已选中时，父分组和叶子都应进入 checked'
);
assertEqual(checkedState.halfChecked, [], '全部后代已选中时不应有 halfChecked');
assertEqual(isGroupFullyChecked(appGroup!, selectedAfterGroupCheck), true, '全部后代已选中时，分组应视为 fully checked');

const selectedAfterGroupUncheck = applyTreeNodeCheck(selectedAfterGroupCheck, appGroup!, false);
assertEqual(selectedAfterGroupUncheck, [], '取消勾选分组应移除全部后代 key');
assertEqual(isGroupFullyChecked(appGroup!, selectedAfterGroupUncheck), false, '取消后分组不应再是 fully checked');

const partialState = buildCheckedTreeNodeState(['app:user:1'], tree);
assertEqual(
  partialState.halfChecked,
  ['group:app:user', 'group:app'],
  '仅部分后代选中时，相关分组应进入 halfChecked'
);
assertEqual(isGroupFullyChecked(appGroup!, ['app:user:1']), false, '部分选中时分组不应是 fully checked');

const renamedState = applyRenamedRedisKeyState(
  {
    keys: sampleKeys,
    selectedKey: 'app:user:2',
    selectedKeys: ['app:user:1', 'app:user:2', 'misc'],
  },
  'app:user:2',
  'app:user:200'
);

assertEqual(
  renamedState.keys.map((item) => item.key),
  ['app:user:1', 'app:user:200', 'app:order:1', 'misc'],
  '重命名后 keys 列表应替换旧 key'
);
assertEqual(renamedState.selectedKey, 'app:user:200', '当前详情选中的 key 应切换为新 key');
assertEqual(
  renamedState.selectedKeys,
  ['app:user:1', 'app:user:200', 'misc'],
  '批量选中集合中的旧 key 应映射为新 key'
);

const unrelatedRenameState = applyRenamedRedisKeyState(
  {
    keys: sampleKeys,
    selectedKey: 'misc',
    selectedKeys: ['app:user:1'],
  },
  'app:order:1',
  'app:order:9'
);
assertEqual(unrelatedRenameState.selectedKey, 'misc', '非当前详情 key 的重命名不应影响 selectedKey');
assertEqual(unrelatedRenameState.selectedKeys, ['app:user:1'], '非已勾选 key 的重命名不应污染选中集合');

console.log('redisViewerTree tests passed');
