import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { createPortal } from 'react-dom';
import { Table, Input, Button, Space, Tag, Tree, Spin, message, Modal, Form, InputNumber, Popconfirm, Tooltip, Radio } from 'antd';
import { ReloadOutlined, DeleteOutlined, PlusOutlined, EditOutlined, SearchOutlined, ClockCircleOutlined, CopyOutlined, FolderOpenOutlined, KeyOutlined, RightOutlined, DownOutlined } from '@ant-design/icons';
import { useStore } from '../store';
import { RedisKeyInfo, RedisValue, StreamEntry } from '../types';
import Editor from '@monaco-editor/react';
import type { DataNode } from 'antd/es/tree';
import { blurToFilter, normalizeBlurForPlatform, normalizeOpacityForPlatform, resolveAppearanceValues } from '../utils/appearance';
import {
    applyRenamedRedisKeyState,
    applyTreeNodeCheck,
    buildLeafNodeKey,
    buildCheckedTreeNodeState,
    buildRedisKeyTree,
    isGroupFullyChecked,
    parseRawKeyFromNodeKey,
    type RedisTreeDataNode,
} from './redisViewerTree';
import { buildRedisWorkbenchTheme } from './redisViewerWorkbenchTheme';

const { Search } = Input;

const REDIS_TREE_KEY_TYPE_WIDTH = 92;
const REDIS_TREE_KEY_TYPE_WIDTH_NARROW = 84;
const REDIS_TREE_KEY_TTL_WIDTH = 92;
const REDIS_TREE_HIDE_TTL_THRESHOLD = 460;
const REDIS_KEY_INITIAL_LOAD_COUNT = 2000;
const REDIS_KEY_LOAD_MORE_COUNT = 2000;
const REDIS_KEY_SEARCH_INITIAL_LOAD_COUNT = 600;
const REDIS_KEY_SEARCH_LOAD_MORE_COUNT = 1000;
const REDIS_LARGE_KEYSPACE_THRESHOLD = 10000;
const REDIS_LARGE_KEYSPACE_MAX_EXPANDED_GROUPS = 200;
const REDIS_KEY_GONE_MESSAGE = 'Redis Key 不存在或已过期';

interface RedisViewerProps {
    connectionId: string;
    redisDB: number;
}

// 尝试多种方式解码二进制数据
const tryDecodeValue = (value: string): { displayValue: string; encoding: string; needsHex: boolean } => {
    if (!value || value.length === 0) {
        return { displayValue: '', encoding: 'UTF-8', needsHex: false };
    }

    // 统计字节分布
    let nullCount = 0;
    let printableCount = 0;
    let highByteCount = 0;
    const sampleSize = Math.min(value.length, 200);

    for (let i = 0; i < sampleSize; i++) {
        const code = value.charCodeAt(i);
        if (code === 0) {
            nullCount++;
        } else if (code >= 32 && code < 127) {
            printableCount++;
        } else if (code >= 128) {
            highByteCount++;
        }
    }

    // 如果超过30%是null字节，很可能是二进制数据，显示十六进制
    if (nullCount / sampleSize > 0.3) {
        return { displayValue: toHexDisplay(value), encoding: 'HEX', needsHex: true };
    }

    // 如果超过70%是可打印ASCII字符，直接显示
    if (printableCount / sampleSize > 0.7) {
        return { displayValue: value, encoding: 'UTF-8', needsHex: false };
    }

    // 尝试UTF-8解码
    if (highByteCount > 0) {
        try {
            const bytes = new Uint8Array(value.length);
            for (let i = 0; i < value.length; i++) {
                bytes[i] = value.charCodeAt(i) & 0xFF;
            }
            const decoded = new TextDecoder('utf-8', { fatal: false }).decode(bytes);

            // 检查解码质量
            let validChars = 0;
            let replacementChars = 0;
            let controlChars = 0;

            for (let i = 0; i < Math.min(decoded.length, 200); i++) {
                const code = decoded.charCodeAt(i);
                if (code === 0xFFFD) {
                    replacementChars++;
                } else if (code < 32 && code !== 9 && code !== 10 && code !== 13) {
                    controlChars++;
                } else if ((code >= 32 && code < 127) || (code >= 0x4E00 && code <= 0x9FFF) || (code >= 0x3000 && code <= 0x303F)) {
                    // ASCII可打印字符、中文字符、中文标点
                    validChars++;
                }
            }

            const totalChecked = Math.min(decoded.length, 200);

            // 如果替换字符超过10%或控制字符超过20%，说明不是有效的UTF-8文本
            if (replacementChars / totalChecked > 0.1 || controlChars / totalChecked > 0.2) {
                return { displayValue: toHexDisplay(value), encoding: 'HEX', needsHex: true };
            }

            // 如果有效字符超过50%，使用UTF-8解码
            if (validChars / totalChecked > 0.5) {
                return { displayValue: decoded, encoding: 'UTF-8', needsHex: false };
            }
        } catch (e) {
            // UTF-8解码失败
        }
    }

    // 默认显示十六进制
    return { displayValue: toHexDisplay(value), encoding: 'HEX', needsHex: true };
};

// 检测是否为二进制数据（包含大量不可打印字符）
const isBinaryData = (value: string): boolean => {
    if (!value || value.length === 0) return false;
    // 检查前 100 个字符中不可打印字符的比例
    const sampleSize = Math.min(value.length, 100);
    let nonPrintableCount = 0;
    for (let i = 0; i < sampleSize; i++) {
        const code = value.charCodeAt(i);
        // 不可打印字符：控制字符（0-31，除了 9, 10, 13）和 DEL（127）
        if ((code < 32 && code !== 9 && code !== 10 && code !== 13) || code === 127 || code > 255) {
            nonPrintableCount++;
        }
    }
    // 如果超过 10% 是不可打印字符，认为是二进制数据
    return nonPrintableCount / sampleSize > 0.1;
};

// 将字符串转换为十六进制显示
const toHexDisplay = (value: string): string => {
    const bytes: string[] = [];
    const ascii: string[] = [];
    let result = '';

    for (let i = 0; i < value.length; i++) {
        const code = value.charCodeAt(i);
        bytes.push(code.toString(16).padStart(2, '0').toUpperCase());
        // 可打印 ASCII 字符显示原字符，否则显示点
        ascii.push(code >= 32 && code < 127 ? value[i] : '.');

        if (bytes.length === 16 || i === value.length - 1) {
            const offset = (Math.floor(i / 16) * 16).toString(16).padStart(8, '0').toUpperCase();
            const hexPart = bytes.join(' ').padEnd(47, ' ');
            const asciiPart = ascii.join('');
            result += `${offset}  ${hexPart}  |${asciiPart}|\n`;
            bytes.length = 0;
            ascii.length = 0;
        }
    }
    return result;
};

// 尝试解析并格式化 JSON
const tryFormatJson = (value: string): { isJson: boolean; formatted: string } => {
    try {
        const parsed = JSON.parse(value);
        return { isJson: true, formatted: JSON.stringify(parsed, null, 2) };
    } catch {
        return { isJson: false, formatted: value };
    }
};

// 格式化字符串值 - 支持 JSON、二进制数据检测和智能解码
const formatStringValue = (value: string): { displayValue: string; isBinary: boolean; isJson: boolean; encoding?: string } => {
    // 先检测是否为二进制数据
    if (isBinaryData(value)) {
        const { displayValue, encoding, needsHex } = tryDecodeValue(value);
        return { displayValue, isBinary: needsHex, isJson: false, encoding };
    }
    // 尝试 JSON 格式化
    const { isJson, formatted } = tryFormatJson(value);
    return { displayValue: formatted, isBinary: false, isJson, encoding: 'UTF-8' };
};

// 可拖拽分隔条组件 - 使用直接 DOM 操作避免卡顿
const ResizableDivider: React.FC<{
    onResizeEnd: (newWidth: number) => void;
    targetRef: React.RefObject<HTMLDivElement>;
    minWidth?: number;
}> = ({ onResizeEnd, targetRef, minWidth = 300 }) => {
    const handleMouseDown = (e: React.MouseEvent) => {
        e.preventDefault();
        e.stopPropagation();

        const target = targetRef.current;
        if (!target) return;

        const startX = e.clientX;
        const startWidth = target.offsetWidth;
        const containerWidth = target.parentElement?.offsetWidth || window.innerWidth;
        const maxWidth = containerWidth - 350; // 右侧至少留 350px

        // 创建遮罩层防止文本选择和其他交互
        const overlay = document.createElement('div');
        overlay.style.cssText = 'position:fixed;top:0;left:0;right:0;bottom:0;cursor:col-resize;z-index:9999;';
        document.body.appendChild(overlay);

        let currentWidth = startWidth;

        const handleMouseMove = (moveEvent: MouseEvent) => {
            moveEvent.preventDefault();
            const delta = moveEvent.clientX - startX;
            currentWidth = Math.max(minWidth, Math.min(maxWidth, startWidth + delta));
            // 直接操作 DOM，不触发 React 重渲染
            target.style.width = `${currentWidth}px`;
            target.style.flexBasis = `${currentWidth}px`;
        };

        const handleMouseUp = () => {
            document.removeEventListener('mousemove', handleMouseMove);
            document.removeEventListener('mouseup', handleMouseUp);
            document.body.removeChild(overlay);
            // 拖拽结束时才更新 React state
            onResizeEnd(currentWidth);
        };

        document.addEventListener('mousemove', handleMouseMove);
        document.addEventListener('mouseup', handleMouseUp);
    };

    return (
        <div
            onMouseDown={handleMouseDown}
            style={{
                width: 5,
                cursor: 'col-resize',
                background: 'transparent',
                flexShrink: 0,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                zIndex: 10,
            }}
            title="拖动调整宽度"
        >
        </div>
    );
};

const getRedisScanLoadCount = (pattern: string, append: boolean): number => {
    const normalizedPattern = pattern.trim() || '*';
    if (normalizedPattern === '*') {
        return append ? REDIS_KEY_LOAD_MORE_COUNT : REDIS_KEY_INITIAL_LOAD_COUNT;
    }
    return append ? REDIS_KEY_SEARCH_LOAD_MORE_COUNT : REDIS_KEY_SEARCH_INITIAL_LOAD_COUNT;
};

const normalizeRedisCursor = (value: unknown): string => {
    if (typeof value === 'string') {
        const trimmed = value.trim();
        return trimmed === '' ? '0' : trimmed;
    }
    if (typeof value === 'number') {
        if (!Number.isFinite(value)) {
            return '0';
        }
        return Math.trunc(value).toString();
    }
    if (typeof value === 'bigint') {
        return value.toString();
    }
    return '0';
};

const isRedisKeyGoneErrorMessage = (messageText: string): boolean => {
    return messageText.includes(REDIS_KEY_GONE_MESSAGE);
};

const RedisViewer: React.FC<RedisViewerProps> = ({ connectionId, redisDB }) => {
    const connections = useStore(state => state.connections);
    const theme = useStore(state => state.theme);
    const appearance = useStore(state => state.appearance);
    const darkMode = theme === 'dark';
    const resolvedAppearance = resolveAppearanceValues(appearance);
    const opacity = normalizeOpacityForPlatform(resolvedAppearance.opacity);
    const blur = normalizeBlurForPlatform(resolvedAppearance.blur);
    const connection = connections.find(c => c.id === connectionId);
    const workbenchTheme = useMemo(() => buildRedisWorkbenchTheme({ darkMode, opacity, blur }), [blur, darkMode, opacity]);
    const keyAccentColor = workbenchTheme.accent;
    const jsonAccentColor = darkMode ? '#f6c453' : '#1890ff';
    const valueToolbarBg = workbenchTheme.panelBgStrong;
    const valueToolbarBorder = workbenchTheme.panelBorder;
    const valueToolbarText = workbenchTheme.textMuted;

    const [keys, setKeys] = useState<RedisKeyInfo[]>([]);
    const [loading, setLoading] = useState(false);
    const [searchPattern, setSearchPattern] = useState('*');
    const [cursor, setCursor] = useState<string>('0');
    const [hasMore, setHasMore] = useState(false);
    const [selectedKey, setSelectedKey] = useState<string | null>(null);
    const [keyValue, setKeyValue] = useState<RedisValue | null>(null);
    const [valueLoading, setValueLoading] = useState(false);
    const [editModalOpen, setEditModalOpen] = useState(false);
    const [newKeyModalOpen, setNewKeyModalOpen] = useState(false);
    const [newKeyForm] = Form.useForm();
    const [renameKeyModalOpen, setRenameKeyModalOpen] = useState(false);
    const [renameKeyForm] = Form.useForm();
    const [renameTargetKey, setRenameTargetKey] = useState<string | null>(null);
    const [ttlModalOpen, setTtlModalOpen] = useState(false);
    const [ttlForm] = Form.useForm();
    const [selectedKeys, setSelectedKeys] = useState<string[]>([]);
    const [editValue, setEditValue] = useState('');
    const [treeContextMenu, setTreeContextMenu] = useState<{ x: number; y: number; rawKey: string } | null>(null);

    // 视图模式状态（用于所有数据类型）
    const [viewMode, setViewMode] = useState<'auto' | 'text' | 'utf8' | 'hex'>('auto');

    // JSON 编辑弹窗状态
    const [jsonEditModalOpen, setJsonEditModalOpen] = useState(false);
    const [jsonEditConfig, setJsonEditConfig] = useState<{
        title: string;
        value: string;
        isJson: boolean;
        onSave: (newValue: string) => Promise<void>;
    } | null>(null);
    const jsonEditValueRef = useRef<string>('');
    const latestLoadRequestIdRef = useRef(0);

    // 面板宽度状态和 ref - 默认占据 50% 宽度
    const [leftPanelWidth, setLeftPanelWidth] = useState<number | string>('50%');
    const leftPanelRef = useRef<HTMLDivElement>(null);
    const treeContainerRef = useRef<HTMLDivElement>(null);
    const [showTreeKeyTTL, setShowTreeKeyTTL] = useState(true);
    const [treeHeight, setTreeHeight] = useState(500);
    const [expandedGroupKeys, setExpandedGroupKeys] = useState<string[]>([]);

    const workbenchCardStyle = useMemo(() => ({
        background: workbenchTheme.panelBg,
        border: workbenchTheme.panelBorder,
        boxShadow: `${workbenchTheme.panelInset}, ${workbenchTheme.shadow}`,
        borderRadius: 18,
        backdropFilter: workbenchTheme.backdropFilter,
        WebkitBackdropFilter: workbenchTheme.backdropFilter,
    }), [workbenchTheme]);

    const workbenchSubCardStyle = useMemo(() => ({
        background: workbenchTheme.panelBgStrong,
        border: workbenchTheme.panelBorder,
        boxShadow: workbenchTheme.panelInset,
        borderRadius: 16,
        backdropFilter: workbenchTheme.backdropFilter,
        WebkitBackdropFilter: workbenchTheme.backdropFilter,
    }), [workbenchTheme]);

    const actionButtonStyle = useMemo(() => ({
        height: 36,
        borderRadius: 12,
        background: workbenchTheme.actionSecondaryBg,
        borderColor: workbenchTheme.actionSecondaryBorder,
        color: workbenchTheme.textPrimary,
        fontWeight: 600,
        boxShadow: 'none',
    }), [workbenchTheme]);

    const primaryActionButtonStyle = useMemo(() => ({
        ...actionButtonStyle,
        background: workbenchTheme.toolbarPrimaryBg,
        borderColor: workbenchTheme.accentBorder,
        color: workbenchTheme.accent,
    }), [actionButtonStyle, workbenchTheme]);

    const dangerActionButtonStyle = useMemo(() => ({
        ...actionButtonStyle,
        background: workbenchTheme.actionDangerBg,
        borderColor: workbenchTheme.actionDangerBorder,
        color: workbenchTheme.actionDangerText,
    }), [actionButtonStyle, workbenchTheme]);

    const pillTagStyle = useMemo(() => ({
        margin: 0,
        borderRadius: 999,
        borderColor: workbenchTheme.statusTagBorder,
        background: workbenchTheme.statusTagBg,
        color: workbenchTheme.isDark ? '#9bc2ff' : '#165dca',
        fontWeight: 600,
        paddingInline: 10,
    }), [workbenchTheme]);

    const mutedPillTagStyle = useMemo(() => ({
        margin: 0,
        borderRadius: 999,
        borderColor: workbenchTheme.statusTagMutedBorder,
        background: workbenchTheme.statusTagMutedBg,
        color: workbenchTheme.textSecondary,
        fontWeight: 500,
        paddingInline: 10,
    }), [workbenchTheme]);
    const redisModalContentStyle = useMemo(() => ({
        background: workbenchTheme.panelBgStrong,
        border: workbenchTheme.panelBorder,
        boxShadow: `${workbenchTheme.panelInset}, ${workbenchTheme.shadow}`,
        backdropFilter: workbenchTheme.backdropFilter,
        WebkitBackdropFilter: workbenchTheme.backdropFilter,
    }), [workbenchTheme]);

    const getConfig = useCallback(() => {
        if (!connection) return null;
        return {
            ...connection.config,
            port: Number(connection.config.port),
            password: connection.config.password || "",
            useSSH: connection.config.useSSH || false,
            ssh: connection.config.ssh || { host: "", port: 22, user: "", password: "", keyPath: "" },
            redisDB: redisDB
        };
    }, [connection, redisDB]);

    const loadKeys = useCallback(async (
        pattern: string = '*',
        fromCursor: string = '0',
        append: boolean = false,
        targetCount?: number
    ) => {
        const config = getConfig();
        if (!config) return;

        const normalizedPattern = pattern.trim() || '*';
        const effectiveTargetCount = targetCount ?? getRedisScanLoadCount(normalizedPattern, append);
        const requestId = latestLoadRequestIdRef.current + 1;
        latestLoadRequestIdRef.current = requestId;

        setLoading(true);
        try {
            const res = await (window as any).go.app.App.RedisScanKeys(config, normalizedPattern, fromCursor, effectiveTargetCount);
            if (requestId !== latestLoadRequestIdRef.current) {
                return;
            }
            if (res.success) {
                const result = res.data;
                const scannedKeys = Array.isArray(result?.keys) ? result.keys : [];
                const nextCursor = normalizeRedisCursor(result?.cursor);
                if (append) {
                    setKeys(prev => {
                        const keyMap = new Map<string, RedisKeyInfo>();
                        prev.forEach(item => keyMap.set(item.key, item));
                        scannedKeys.forEach((item: RedisKeyInfo) => keyMap.set(item.key, item));
                        return Array.from(keyMap.values());
                    });
                } else {
                    setKeys(scannedKeys);
                }
                setCursor(nextCursor);
                setHasMore(nextCursor !== '0');
            } else {
                message.error('加载 Key 失败: ' + res.message);
            }
        } catch (e: any) {
            if (requestId !== latestLoadRequestIdRef.current) {
                return;
            }
            message.error('加载 Key 失败: ' + (e?.message || String(e)));
        } finally {
            if (requestId === latestLoadRequestIdRef.current) {
                setLoading(false);
            }
        }
    }, [getConfig]);

    useEffect(() => {
        loadKeys(searchPattern, '0', false, getRedisScanLoadCount(searchPattern, false));
    }, [redisDB]);

    const handleSearch = (value: string) => {
        const pattern = value.trim() || '*';
        setSearchPattern(pattern);
        setCursor('0');
        loadKeys(pattern, '0', false, getRedisScanLoadCount(pattern, false));
    };

    const handleLoadMore = () => {
        if (!hasMore || loading) {
            return;
        }
        loadKeys(searchPattern, cursor, true, getRedisScanLoadCount(searchPattern, true));
    };

    const handleRefresh = () => {
        setCursor('0');
        loadKeys(searchPattern, '0', false, getRedisScanLoadCount(searchPattern, false));
    };

    const handleSelectAllLoadedKeys = useCallback(() => {
        setSelectedKeys(keys.map((item) => item.key));
    }, [keys]);

    const handleClearAllSelectedKeys = useCallback(() => {
        setSelectedKeys([]);
    }, []);

    const removeMissingKeyFromView = useCallback((missingKey: string) => {
        setKeys(prev => prev.filter(item => item.key !== missingKey));
        setSelectedKeys(prev => prev.filter(item => item !== missingKey));
        setSelectedKey(null);
        setKeyValue(null);
    }, []);

    const loadKeyValue = async (key: string) => {
        const config = getConfig();
        if (!config) return;

        setValueLoading(true);
        try {
            const res = await (window as any).go.app.App.RedisGetValue(config, key);
            if (res.success) {
                setKeyValue(res.data);
                setSelectedKey(key);
            } else {
                const messageText = String(res.message || '');
                if (isRedisKeyGoneErrorMessage(messageText)) {
                    removeMissingKeyFromView(key);
                    message.warning('Key 已不存在或已过期，已从列表移除');
                } else {
                    message.error('获取值失败: ' + messageText);
                }
            }
        } catch (e: any) {
            const messageText = e?.message || String(e);
            if (isRedisKeyGoneErrorMessage(messageText)) {
                removeMissingKeyFromView(key);
                message.warning('Key 已不存在或已过期，已从列表移除');
            } else {
                message.error('获取值失败: ' + messageText);
            }
        } finally {
            setValueLoading(false);
        }
    };

    const handleDeleteKeys = async (keysToDelete: string[]) => {
        const config = getConfig();
        if (!config) return;

        try {
            const res = await (window as any).go.app.App.RedisDeleteKeys(config, keysToDelete);
            if (res.success) {
                message.success(`已删除 ${res.data.deleted} 个 Key`);
                setKeys(prev => prev.filter(k => !keysToDelete.includes(k.key)));
                if (selectedKey && keysToDelete.includes(selectedKey)) {
                    setSelectedKey(null);
                    setKeyValue(null);
                }
                setSelectedKeys([]);
            } else {
                message.error('删除失败: ' + res.message);
            }
        } catch (e: any) {
            message.error('删除失败: ' + (e?.message || String(e)));
        }
    };

    const handleDeleteCurrentKey = async () => {
        if (!selectedKey) return;
        await handleDeleteKeys([selectedKey]);
    };

    const handleSetTTL = async () => {
        const config = getConfig();
        if (!config || !selectedKey) return;

        try {
            const values = await ttlForm.validateFields();
            const res = await (window as any).go.app.App.RedisSetTTL(config, selectedKey, values.ttl);
            if (res.success) {
                message.success('TTL 设置成功');
                setTtlModalOpen(false);
                loadKeyValue(selectedKey);
                handleRefresh();
            } else {
                message.error('设置失败: ' + res.message);
            }
        } catch (e: any) {
            message.error('设置失败: ' + (e?.message || String(e)));
        }
    };

    const handleSaveString = async () => {
        const config = getConfig();
        if (!config || !selectedKey) return;

        try {
            const res = await (window as any).go.app.App.RedisSetString(config, selectedKey, editValue, keyValue?.ttl || -1);
            if (res.success) {
                message.success('保存成功');
                setEditModalOpen(false);
                loadKeyValue(selectedKey);
            } else {
                message.error('保存失败: ' + res.message);
            }
        } catch (e: any) {
            message.error('保存失败: ' + (e?.message || String(e)));
        }
    };

    const handleCreateKey = async () => {
        const config = getConfig();
        if (!config) return;

        try {
            const values = await newKeyForm.validateFields();
            const res = await (window as any).go.app.App.RedisSetString(config, values.key, values.value, values.ttl || -1);
            if (res.success) {
                message.success('创建成功');
                setNewKeyModalOpen(false);
                newKeyForm.resetFields();
                handleRefresh();
            } else {
                message.error('创建失败: ' + res.message);
            }
        } catch (e: any) {
            message.error('创建失败: ' + (e?.message || String(e)));
        }
    };

    const openRenameKeyModal = useCallback((rawKey: string) => {
        setTreeContextMenu(null);
        setRenameTargetKey(rawKey);
        renameKeyForm.setFieldsValue({ key: rawKey });
        setRenameKeyModalOpen(true);
    }, [renameKeyForm]);

    const handleRenameKey = async () => {
        const config = getConfig();
        if (!config || !renameTargetKey) return;

        try {
            const values = await renameKeyForm.validateFields();
            const nextKey = String(values.key || '').trim();
            if (!nextKey) {
                message.warning('请输入新的 Key 名称');
                return;
            }
            if (nextKey === renameTargetKey) {
                message.warning('新的 Key 名称不能与原值相同');
                return;
            }

            const existsRes = await (window as any).go.app.App.RedisKeyExists(config, nextKey);
            if (!existsRes?.success) {
                message.error('校验目标 Key 失败: ' + (existsRes?.message || '未知错误'));
                return;
            }
            if (existsRes?.data?.exists) {
                message.error(`目标 Key 已存在: ${nextKey}`);
                return;
            }

            const res = await (window as any).go.app.App.RedisRenameKey(config, renameTargetKey, nextKey);
            if (res.success) {
                const nextState = applyRenamedRedisKeyState(
                    {
                        keys,
                        selectedKey,
                        selectedKeys,
                    },
                    renameTargetKey,
                    nextKey
                );
                setKeys(nextState.keys);
                setSelectedKey(nextState.selectedKey);
                setSelectedKeys(Array.from(new Set(nextState.selectedKeys)));
                setRenameKeyModalOpen(false);
                setRenameTargetKey(null);
                renameKeyForm.resetFields();
                message.success('Key 重命名成功');
                if (selectedKey === renameTargetKey) {
                    void loadKeyValue(nextKey);
                }
                handleRefresh();
            } else {
                message.error('重命名失败: ' + res.message);
            }
        } catch (e: any) {
            message.error('重命名失败: ' + (e?.message || String(e)));
        }
    };

    const getTypeColor = (type: string) => {
        switch (type) {
            case 'string': return 'green';
            case 'hash': return 'blue';
            case 'list': return 'orange';
            case 'set': return 'purple';
            case 'zset': return 'magenta';
            case 'stream': return 'cyan';
            default: return 'default';
        }
    };

    const formatTTL = (ttl: number) => {
        if (ttl === -1) return '永久';
        if (ttl === -2) return '已过期';
        if (ttl < 60) return `${ttl}秒`;
        if (ttl < 3600) return `${Math.floor(ttl / 60)}分${ttl % 60}秒`;
        if (ttl < 86400) return `${Math.floor(ttl / 3600)}时${Math.floor((ttl % 3600) / 60)}分`;
        return `${Math.floor(ttl / 86400)}天${Math.floor((ttl % 86400) / 3600)}时`;
    };

    useEffect(() => {
        const target = leftPanelRef.current;
        if (!target) return;

        const updateTTLVisibility = (width: number) => {
            const nextShowTTL = width > REDIS_TREE_HIDE_TTL_THRESHOLD;
            setShowTreeKeyTTL((prev) => (prev === nextShowTTL ? prev : nextShowTTL));
        };

        updateTTLVisibility(Math.round(target.getBoundingClientRect().width));

        if (typeof ResizeObserver !== 'undefined') {
            const observer = new ResizeObserver((entries) => {
                const width = Math.round(entries[0]?.contentRect.width || target.getBoundingClientRect().width);
                updateTTLVisibility(width);
            });
            observer.observe(target);
            return () => observer.disconnect();
        }

        const handleWindowResize = () => {
            updateTTLVisibility(Math.round(target.getBoundingClientRect().width));
        };
        window.addEventListener('resize', handleWindowResize);
        return () => window.removeEventListener('resize', handleWindowResize);
    }, []);

    useEffect(() => {
        const target = treeContainerRef.current;
        if (!target) return;

        const updateTreeHeight = (nextHeight: number) => {
            if (nextHeight <= 0) return;
            setTreeHeight((prev) => (prev === nextHeight ? prev : nextHeight));
        };

        updateTreeHeight(Math.round(target.getBoundingClientRect().height));

        if (typeof ResizeObserver !== 'undefined') {
            const observer = new ResizeObserver((entries) => {
                const nextHeight = Math.round(entries[0]?.contentRect.height || target.getBoundingClientRect().height);
                updateTreeHeight(nextHeight);
            });
            observer.observe(target);
            return () => observer.disconnect();
        }

        const handleWindowResize = () => {
            updateTreeHeight(Math.round(target.getBoundingClientRect().height));
        };
        window.addEventListener('resize', handleWindowResize);
        return () => window.removeEventListener('resize', handleWindowResize);
    }, []);

    const isLargeKeyspace = keys.length >= REDIS_LARGE_KEYSPACE_THRESHOLD;

    const keyTree = useMemo(() => {
        return buildRedisKeyTree(keys, !isLargeKeyspace);
    }, [isLargeKeyspace, keys]);

    const groupKeySet = useMemo(() => new Set(keyTree.groupKeys), [keyTree.groupKeys]);

    const selectedTreeNodeKeys = useMemo(() => {
        if (!selectedKey) {
            return [] as string[];
        }
        return [buildLeafNodeKey(selectedKey)];
    }, [selectedKey]);

    const checkedTreeNodeKeys = useMemo(() => {
        return buildCheckedTreeNodeState(selectedKeys, keyTree);
    }, [keyTree, selectedKeys]);

    useEffect(() => {
        const existingKeySet = new Set(keys.map(item => item.key));
        setSelectedKeys(prev => prev.filter(rawKey => existingKeySet.has(rawKey)));
    }, [keys]);

    useEffect(() => {
        setExpandedGroupKeys((prev) => {
            const validKeys = prev.filter(nodeKey => groupKeySet.has(nodeKey));
            if (!isLargeKeyspace) {
                return validKeys;
            }
            return validKeys.slice(0, REDIS_LARGE_KEYSPACE_MAX_EXPANDED_GROUPS);
        });
    }, [groupKeySet, isLargeKeyspace]);

    useEffect(() => {
        if (!treeContextMenu) {
            return;
        }
        const handleDismiss = () => setTreeContextMenu(null);
        window.addEventListener('click', handleDismiss);
        window.addEventListener('scroll', handleDismiss, true);
        window.addEventListener('contextmenu', handleDismiss);
        return () => {
            window.removeEventListener('click', handleDismiss);
            window.removeEventListener('scroll', handleDismiss, true);
            window.removeEventListener('contextmenu', handleDismiss);
        };
    }, [treeContextMenu]);

    const handleTreeSelect = (nodeKeys: React.Key[]) => {
        if (nodeKeys.length === 0) {
            return;
        }
        const rawKey = parseRawKeyFromNodeKey(nodeKeys[0]);
        if (!rawKey) {
            return;
        }
        loadKeyValue(rawKey);
    };

    const handleTreeCheck = (
        _checked: React.Key[] | { checked: React.Key[]; halfChecked: React.Key[] },
        info: { checked: boolean; node: DataNode }
    ) => {
        const node = info.node as RedisTreeDataNode;
        setSelectedKeys((prev) => applyTreeNodeCheck(prev, node, info.checked));
    };

    const handleTreeRightClick = ({ event, node }: { event: React.MouseEvent; node: DataNode }) => {
        event.preventDefault();
        event.stopPropagation();
        const treeNode = node as RedisTreeDataNode;
        if (treeNode.nodeType !== 'leaf' || !treeNode.rawKey) {
            setTreeContextMenu(null);
            return;
        }

        setTreeContextMenu({
            x: event.clientX,
            y: event.clientY,
            rawKey: treeNode.rawKey,
        });
    };

    const handleSelectGroupDescendants = useCallback((treeNode: RedisTreeDataNode) => {
        setSelectedKeys((prev) => applyTreeNodeCheck(prev, treeNode, !isGroupFullyChecked(treeNode, prev)));
    }, []);

    const handleToggleGroupExpand = useCallback((groupNodeKey: string) => {
        setExpandedGroupKeys((prev) => {
            const exists = prev.includes(groupNodeKey);
            const nextKeys = exists
                ? prev.filter((nodeKey) => nodeKey !== groupNodeKey)
                : [...prev, groupNodeKey];

            if (isLargeKeyspace) {
                return nextKeys.slice(-REDIS_LARGE_KEYSPACE_MAX_EXPANDED_GROUPS);
            }

            return nextKeys;
        });
    }, [isLargeKeyspace]);

    const stopTreeTitleEvent = (event: React.MouseEvent<HTMLElement>) => {
        event.preventDefault();
        event.stopPropagation();
    };

    const renderTreeNodeTitle = useCallback((nodeData: DataNode) => {
        const treeNode = nodeData as RedisTreeDataNode;

        if (treeNode.nodeType === 'group') {
            const groupFullyChecked = isGroupFullyChecked(treeNode, selectedKeys);
            const groupNodeKey = String(treeNode.key ?? '');
            const isExpanded = expandedGroupKeys.includes(groupNodeKey);
            return (
                <div
                    style={{
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'space-between',
                        gap: 8,
                        width: '100%',
                        minWidth: 0,
                        padding: '2px 0',
                    }}
                >
                    <Space size={6} style={{ minWidth: 0, overflow: 'hidden' }}>
                        <button
                            type="button"
                            className="redis-tree-expander-button"
                            aria-label={isExpanded ? '折叠分组' : '展开分组'}
                            onMouseDown={stopTreeTitleEvent}
                            onClick={(event) => {
                                stopTreeTitleEvent(event);
                                handleToggleGroupExpand(groupNodeKey);
                            }}
                            style={{
                                width: 18,
                                height: 18,
                                padding: 0,
                                border: 'none',
                                background: 'transparent',
                                color: workbenchTheme.textMuted,
                                display: 'inline-flex',
                                alignItems: 'center',
                                justifyContent: 'center',
                                borderRadius: 6,
                                cursor: 'pointer',
                                flexShrink: 0,
                            }}
                        >
                            {isExpanded ? <DownOutlined style={{ fontSize: 11 }} /> : <RightOutlined style={{ fontSize: 11 }} />}
                        </button>
                        <FolderOpenOutlined style={{ color: workbenchTheme.textMuted }} />
                        <span style={{ minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                            {treeNode.groupName}
                        </span>
                        <span style={{ fontSize: 12, color: workbenchTheme.textMuted, flexShrink: 0 }}>({treeNode.groupLeafCount ?? 0})</span>
                    </Space>
                    <Button
                        size="small"
                        style={{
                            paddingInline: 10,
                            height: 26,
                            borderRadius: 999,
                            flexShrink: 0,
                            borderColor: workbenchTheme.accentBorder,
                            background: workbenchTheme.accentSoft,
                            color: workbenchTheme.accent,
                            fontWeight: 600,
                        }}
                        onMouseDown={stopTreeTitleEvent}
                        onClick={(event) => {
                            stopTreeTitleEvent(event);
                            handleSelectGroupDescendants(treeNode);
                        }}
                    >
                        {groupFullyChecked ? '取消全选' : '全选'}
                    </Button>
                </div>
            );
        }

        const leafLabel = treeNode.leafLabel ?? '';
        const rawKey = treeNode.rawKey ?? parseRawKeyFromNodeKey(treeNode.key ?? '') ?? '';
        const keyType = treeNode.keyType ?? 'unknown';
        const ttl = typeof treeNode.ttl === 'number' ? treeNode.ttl : -1;

        if (isLargeKeyspace) {
            return (
                <div style={{ minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: workbenchTheme.textPrimary }}>
                    <span>{leafLabel}</span>
                    <span style={{ marginLeft: 8, color: workbenchTheme.textMuted, fontSize: 12 }}>[{keyType}]</span>
                    {showTreeKeyTTL && (
                        <span style={{ marginLeft: 8, color: workbenchTheme.textMuted, fontSize: 12 }}>{formatTTL(ttl)}</span>
                    )}
                </div>
            );
        }

        return (
            <div
                style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: 8,
                    minWidth: 0,
                    width: '100%',
                    overflow: 'hidden',
                }}
            >
                <div
                    style={{
                        display: 'flex',
                        alignItems: 'center',
                        gap: 6,
                        minWidth: 0,
                        flex: 1,
                        overflow: 'hidden',
                    }}
                >
                    <KeyOutlined style={{ color: keyAccentColor, flexShrink: 0 }} />
                    <Tooltip title={rawKey}>
                        <span
                            style={{
                                minWidth: 0,
                                overflow: 'hidden',
                                textOverflow: 'ellipsis',
                                whiteSpace: 'nowrap',
                                display: 'block',
                            }}
                        >
                            {leafLabel}
                        </span>
                    </Tooltip>
                </div>
                <Tag
                    color={getTypeColor(keyType)}
                    style={{
                        marginInlineEnd: 0,
                        width: showTreeKeyTTL ? REDIS_TREE_KEY_TYPE_WIDTH : REDIS_TREE_KEY_TYPE_WIDTH_NARROW,
                        textAlign: 'center',
                        flexShrink: 0,
                        borderRadius: 999,
                        fontWeight: 600,
                    }}
                >
                    {keyType}
                </Tag>
                {showTreeKeyTTL && (
                    <span
                        style={{
                            width: REDIS_TREE_KEY_TTL_WIDTH,
                            fontSize: 12,
                            color: workbenchTheme.textMuted,
                            textAlign: 'left',
                            whiteSpace: 'nowrap',
                            flexShrink: 0,
                            overflow: 'hidden',
                            textOverflow: 'ellipsis',
                        }}
                    >
                        {formatTTL(ttl)}
                    </span>
                )}
            </div>
        );
    }, [expandedGroupKeys, formatTTL, getTypeColor, handleSelectGroupDescendants, handleToggleGroupExpand, isLargeKeyspace, keyAccentColor, selectedKeys, showTreeKeyTTL, workbenchTheme]);

    const handleTreeExpand = (nextExpandedKeys: React.Key[]) => {
        const validGroupKeys = nextExpandedKeys
            .map(key => String(key))
            .filter(nodeKey => groupKeySet.has(nodeKey));
        if (isLargeKeyspace) {
            setExpandedGroupKeys(validGroupKeys.slice(0, REDIS_LARGE_KEYSPACE_MAX_EXPANDED_GROUPS));
            return;
        }
        setExpandedGroupKeys(validGroupKeys);
    };

    const renderValueEditor = () => {
        if (!keyValue || !selectedKey) {
            return (
                <div
                    style={{
                        ...workbenchCardStyle,
                        height: '100%',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        background: workbenchTheme.contentEmptyBg,
                        color: workbenchTheme.textMuted,
                        padding: 24,
                    }}
                >
                    选择一个 Key 查看详情
                </div>
            );
        }

        const renderStringValue = () => {
            const strValue = String(keyValue.value);

            // 根据查看模式生成显示内容
            const getDisplayContent = () => {
                if (viewMode === 'hex') {
                    return { displayValue: toHexDisplay(strValue), isBinary: true, encoding: 'HEX' };
                } else if (viewMode === 'text') {
                    return { displayValue: strValue, isBinary: false, encoding: 'Text' };
                } else if (viewMode === 'utf8') {
                    try {
                        const bytes = new Uint8Array(strValue.length);
                        for (let i = 0; i < strValue.length; i++) {
                            bytes[i] = strValue.charCodeAt(i) & 0xFF;
                        }
                        const decoded = new TextDecoder('utf-8', { fatal: false }).decode(bytes);
                        return { displayValue: decoded, isBinary: false, encoding: 'UTF-8' };
                    } catch (e) {
                        return { displayValue: strValue, isBinary: false, encoding: 'UTF-8 (失败)' };
                    }
                } else {
                    // auto mode
                    const { displayValue, isBinary, isJson, encoding } = formatStringValue(strValue);
                    return { displayValue, isBinary, encoding };
                }
            };

            const { displayValue, isBinary, encoding } = getDisplayContent();
            const isJson = viewMode === 'auto' && formatStringValue(strValue).isJson;

            return (
                <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
                    <div style={{
                        padding: '4px 8px',
                        background: valueToolbarBg,
                        borderBottom: valueToolbarBorder,
                        display: 'flex',
                        alignItems: 'center'
                    }}>
                        <span style={{ fontSize: 12, color: valueToolbarText }}>
                            {encoding && `编码: ${encoding}`}
                        </span>
                    </div>
                    <Editor
                        height="calc(100% - 72px)"
                        language={isJson ? 'json' : 'plaintext'}
                        theme={darkMode ? 'transparent-dark' : 'transparent-light'}
                        value={displayValue}
                        options={{
                            readOnly: true,
                            minimap: { enabled: false },
                            lineNumbers: 'on',
                            wordWrap: isBinary ? 'off' : 'on',
                            scrollBeyondLastLine: false,
                            automaticLayout: true,
                            folding: true,
                            formatOnPaste: true,
                            fontFamily: isBinary ? 'monospace' : undefined
                        }}
                    />
                    <div style={{ padding: '8px 0', flexShrink: 0 }}>
                        <Space>
                            <Button icon={<CopyOutlined />} onClick={() => {
                                navigator.clipboard.writeText(strValue).then(() => {
                                    message.success('已复制');
                                }).catch(() => {
                                    message.error('复制失败');
                                });
                            }}>复制</Button>
                            {!isBinary && viewMode === 'auto' && (
                                <Button icon={<EditOutlined />} onClick={() => {
                                    setEditValue(displayValue);
                                    setEditModalOpen(true);
                                }}>编辑</Button>
                            )}
                            {(isBinary || viewMode !== 'auto') && (
                                <span style={{ color: '#999', fontSize: 12 }}>
                                    {viewMode !== 'auto' ? '切换到"自动"模式以编辑' : '二进制数据不支持编辑'}
                                </span>
                            )}
                        </Space>
                    </div>
                </div>
            );
        };

        const renderHashValue = () => {
            // 根据查看模式处理值
            const processValue = (value: string) => {
                if (viewMode === 'hex') {
                    return { displayValue: toHexDisplay(value), isBinary: true, isJson: false, encoding: 'HEX' };
                } else if (viewMode === 'text') {
                    return { displayValue: value, isBinary: false, isJson: false, encoding: 'Text' };
                } else if (viewMode === 'utf8') {
                    try {
                        const bytes = new Uint8Array(value.length);
                        for (let i = 0; i < value.length; i++) {
                            bytes[i] = value.charCodeAt(i) & 0xFF;
                        }
                        const decoded = new TextDecoder('utf-8', { fatal: false }).decode(bytes);
                        return { displayValue: decoded, isBinary: false, isJson: false, encoding: 'UTF-8' };
                    } catch (e) {
                        return { displayValue: value, isBinary: false, isJson: false, encoding: 'UTF-8 (失败)' };
                    }
                } else {
                    // auto mode
                    return formatStringValue(value);
                }
            };

            const data = Object.entries(keyValue.value as Record<string, string>).map(([field, value]) => {
                const { displayValue, isBinary, isJson, encoding } = processValue(value);
                return { field, value, displayValue, isBinary, isJson, encoding };
            });

            const handleEditHashField = async (field: string, newValue: string) => {
                const config = getConfig();
                if (!config) return;
                try {
                    const res = await (window as any).go.app.App.RedisSetHashField(config, selectedKey, field, newValue);
                    if (res.success) {
                        message.success('修改成功');
                        loadKeyValue(selectedKey);
                    } else {
                        message.error('修改失败: ' + res.message);
                    }
                } catch (e: any) {
                    message.error('修改失败: ' + (e?.message || String(e)));
                }
            };

            const handleDeleteHashField = async (field: string) => {
                const config = getConfig();
                if (!config) return;
                try {
                    const res = await (window as any).go.app.App.RedisDeleteHashField(config, selectedKey, field);
                    if (res.success) {
                        message.success('删除成功');
                        loadKeyValue(selectedKey);
                    } else {
                        message.error('删除失败: ' + res.message);
                    }
                } catch (e: any) {
                    message.error('删除失败: ' + (e?.message || String(e)));
                }
            };

            return (
                <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
                    <div style={{ marginBottom: 8, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <Button size="small" style={actionButtonStyle} icon={<PlusOutlined />} onClick={() => {
                            Modal.confirm({
                                title: '添加字段',
                                content: (
                                    <Form id="add-hash-field-form" layout="vertical">
                                        <Form.Item label="字段名" name="field" rules={[{ required: true }]}>
                                            <Input id="new-hash-field" />
                                        </Form.Item>
                                        <Form.Item label="值" name="value" rules={[{ required: true }]}>
                                            <Input.TextArea id="new-hash-value" rows={4} />
                                        </Form.Item>
                                    </Form>
                                ),
                                onOk: async () => {
                                    const field = (document.getElementById('new-hash-field') as HTMLInputElement)?.value;
                                    const value = (document.getElementById('new-hash-value') as HTMLTextAreaElement)?.value;
                                    if (field && value !== undefined) {
                                        await handleEditHashField(field, value);
                                    }
                                }
                            });
                        }}>添加字段</Button>
                    </div>
                    <Table
                        dataSource={data}
                        columns={[
                            { title: 'Field', dataIndex: 'field', key: 'field', width: 200, ellipsis: true },
                            {
                                title: 'Value',
                                dataIndex: 'displayValue',
                                key: 'value',
                                ellipsis: true,
                                render: (text: string, record: any) => {
                                    const tooltipContent = record.encoding && record.encoding !== 'UTF-8'
                                        ? `[${record.encoding}]\n${text}`
                                        : text;

                                    return (
                                        <Tooltip title={<pre style={{ maxHeight: 300, overflow: 'auto', margin: 0, fontSize: 12 }}>{tooltipContent}</pre>} styles={{ root: { maxWidth: 600 } }}>
                                            <span style={{
                                                color: record.isBinary ? '#d46b08' : (record.isJson ? jsonAccentColor : undefined),
                                                fontFamily: record.isBinary ? 'monospace' : undefined,
                                                fontSize: record.isBinary ? 11 : undefined
                                            }}>
                                                {text}
                                            </span>
                                        </Tooltip>
                                    );
                                }
                            },
                            {
                                title: '操作',
                                key: 'action',
                                width: 120,
                                render: (_: any, record: any) => (
                                    <Space size="small">
                                        <Tooltip title="复制值">
                                            <Button type="text" size="small" icon={<CopyOutlined />} onClick={() => {
                                                navigator.clipboard.writeText(record.value).then(() => {
                                                    message.success('已复制');
                                                }).catch(() => {
                                                    message.error('复制失败');
                                                });
                                            }} />
                                        </Tooltip>
                                        {!record.isBinary && (
                                            <Button type="text" size="small" icon={<EditOutlined />} onClick={() => {
                                                // 如果是 JSON，格式化显示
                                                const editContent = record.isJson ? record.displayValue : record.value;
                                                setJsonEditConfig({
                                                    title: `编辑字段: ${record.field}`,
                                                    value: editContent,
                                                    isJson: record.isJson,
                                                    onSave: async (newValue: string) => {
                                                        await handleEditHashField(record.field, newValue);
                                                    }
                                                });
                                                setJsonEditModalOpen(true);
                                            }} />
                                        )}
                                        <Popconfirm title="确定删除此字段？" onConfirm={() => handleDeleteHashField(record.field)}>
                                            <Button type="text" size="small" danger icon={<DeleteOutlined />} />
                                        </Popconfirm>
                                    </Space>
                                )
                            }
                        ]}
                        rowKey="field"
                        size="small"
                        pagination={{ pageSize: 50 }}
                        scroll={{ y: 'calc(100vh - 350px)' }}
                        style={{ flex: 1 }}
                    />
                </div>
            );
        };

        const renderListValue = () => {
            // 根据查看模式处理值
            const processValue = (value: string) => {
                if (viewMode === 'hex') {
                    return { displayValue: toHexDisplay(value), isBinary: true, isJson: false, encoding: 'HEX' };
                } else if (viewMode === 'text') {
                    return { displayValue: value, isBinary: false, isJson: false, encoding: 'Text' };
                } else if (viewMode === 'utf8') {
                    try {
                        const bytes = new Uint8Array(value.length);
                        for (let i = 0; i < value.length; i++) {
                            bytes[i] = value.charCodeAt(i) & 0xFF;
                        }
                        const decoded = new TextDecoder('utf-8', { fatal: false }).decode(bytes);
                        return { displayValue: decoded, isBinary: false, isJson: false, encoding: 'UTF-8' };
                    } catch (e) {
                        return { displayValue: value, isBinary: false, isJson: false, encoding: 'UTF-8 (失败)' };
                    }
                } else {
                    // auto mode
                    return formatStringValue(value);
                }
            };

            const data = (keyValue.value as string[]).map((value, index) => {
                const { displayValue, isBinary, isJson, encoding } = processValue(value);
                return { index, value, displayValue, isBinary, isJson, encoding };
            });

            const handleEditListItem = async (index: number, newValue: string) => {
                const config = getConfig();
                if (!config) return;
                try {
                    const res = await (window as any).go.app.App.RedisListSet(config, selectedKey, index, newValue);
                    if (res.success) {
                        message.success('修改成功');
                        loadKeyValue(selectedKey);
                    } else {
                        message.error('修改失败: ' + res.message);
                    }
                } catch (e: any) {
                    message.error('修改失败: ' + (e?.message || String(e)));
                }
            };

            const handleAddListItem = async (value: string, position: 'left' | 'right') => {
                const config = getConfig();
                if (!config) return;
                try {
                    const res = await (window as any).go.app.App.RedisListPush(config, selectedKey, { values: [value], position });
                    if (res.success) {
                        message.success('添加成功');
                        loadKeyValue(selectedKey);
                    } else {
                        message.error('添加失败: ' + res.message);
                    }
                } catch (e: any) {
                    message.error('添加失败: ' + (e?.message || String(e)));
                }
            };

            return (
                <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
                    <div style={{ marginBottom: 8, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <Space>
                            <Button size="small" style={actionButtonStyle} icon={<PlusOutlined />} onClick={() => {
                                Modal.confirm({
                                    title: '添加元素',
                                    content: (
                                        <div>
                                            <Input.TextArea id="new-list-value" rows={4} placeholder="输入新元素值" />
                                        </div>
                                    ),
                                    onOk: async () => {
                                        const value = (document.getElementById('new-list-value') as HTMLTextAreaElement)?.value;
                                        if (value) {
                                            await handleAddListItem(value, 'right');
                                        }
                                    }
                                });
                            }}>添加到尾部</Button>
                            <Button size="small" style={actionButtonStyle} onClick={() => {
                                Modal.confirm({
                                    title: '添加元素到头部',
                                    content: (
                                        <div>
                                            <Input.TextArea id="new-list-value-left" rows={4} placeholder="输入新元素值" />
                                        </div>
                                    ),
                                    onOk: async () => {
                                        const value = (document.getElementById('new-list-value-left') as HTMLTextAreaElement)?.value;
                                        if (value) {
                                            await handleAddListItem(value, 'left');
                                        }
                                    }
                                });
                            }}>添加到头部</Button>
                        </Space>
                    </div>
                    <Table
                        dataSource={data}
                        columns={[
                            { title: '索引', dataIndex: 'index', key: 'index', width: 80 },
                            {
                                title: '值',
                                dataIndex: 'displayValue',
                                key: 'value',
                                ellipsis: true,
                                render: (text: string, record: any) => {
                                    const tooltipContent = record.encoding && record.encoding !== 'UTF-8'
                                        ? `[${record.encoding}]\n${text}`
                                        : text;

                                    return (
                                        <Tooltip title={<pre style={{ maxHeight: 300, overflow: 'auto', margin: 0, fontSize: 12 }}>{tooltipContent}</pre>} styles={{ root: { maxWidth: 600 } }}>
                                            <span style={{
                                                color: record.isBinary ? '#d46b08' : (record.isJson ? jsonAccentColor : undefined),
                                                fontFamily: record.isBinary ? 'monospace' : undefined,
                                                fontSize: record.isBinary ? 11 : undefined
                                            }}>
                                                {text}
                                            </span>
                                        </Tooltip>
                                    );
                                }
                            },
                            {
                                title: '操作',
                                key: 'action',
                                width: 80,
                                render: (_: any, record: any) => (
                                    <Space size="small">
                                        <Tooltip title="复制值">
                                            <Button type="text" size="small" icon={<CopyOutlined />} onClick={() => {
                                                navigator.clipboard.writeText(record.value).then(() => {
                                                    message.success('已复制');
                                                }).catch(() => {
                                                    message.error('复制失败');
                                                });
                                            }} />
                                        </Tooltip>
                                        {!record.isBinary && (
                                            <Button type="text" size="small" icon={<EditOutlined />} onClick={() => {
                                                // 如果是 JSON，格式化显示
                                                const editContent = record.isJson ? record.displayValue : record.value;
                                                setJsonEditConfig({
                                                    title: `编辑索引 ${record.index}`,
                                                    value: editContent,
                                                    isJson: record.isJson,
                                                    onSave: async (newValue: string) => {
                                                        await handleEditListItem(record.index, newValue);
                                                    }
                                                });
                                                setJsonEditModalOpen(true);
                                            }} />
                                        )}
                                    </Space>
                                )
                            }
                        ]}
                        rowKey="index"
                        size="small"
                        pagination={{ pageSize: 50 }}
                        scroll={{ y: 'calc(100vh - 350px)' }}
                        style={{ flex: 1 }}
                    />
                </div>
            );
        };

        const renderSetValue = () => {
            // 根据查看模式处理值
            const processValue = (value: string) => {
                if (viewMode === 'hex') {
                    return { displayValue: toHexDisplay(value), isBinary: true, isJson: false, encoding: 'HEX' };
                } else if (viewMode === 'text') {
                    return { displayValue: value, isBinary: false, isJson: false, encoding: 'Text' };
                } else if (viewMode === 'utf8') {
                    try {
                        const bytes = new Uint8Array(value.length);
                        for (let i = 0; i < value.length; i++) {
                            bytes[i] = value.charCodeAt(i) & 0xFF;
                        }
                        const decoded = new TextDecoder('utf-8', { fatal: false }).decode(bytes);
                        return { displayValue: decoded, isBinary: false, isJson: false, encoding: 'UTF-8' };
                    } catch (e) {
                        return { displayValue: value, isBinary: false, isJson: false, encoding: 'UTF-8 (失败)' };
                    }
                } else {
                    // auto mode
                    return formatStringValue(value);
                }
            };

            const data = (keyValue.value as string[]).map((member, index) => {
                const { displayValue, isBinary, isJson, encoding } = processValue(member);
                return { index, member, displayValue, isBinary, isJson, encoding };
            });

            const handleAddSetMember = async (member: string) => {
                const config = getConfig();
                if (!config) return;
                try {
                    const res = await (window as any).go.app.App.RedisSetAdd(config, selectedKey, [member]);
                    if (res.success) {
                        message.success('添加成功');
                        loadKeyValue(selectedKey);
                    } else {
                        message.error('添加失败: ' + res.message);
                    }
                } catch (e: any) {
                    message.error('添加失败: ' + (e?.message || String(e)));
                }
            };

            const handleRemoveSetMember = async (member: string) => {
                const config = getConfig();
                if (!config) return;
                try {
                    const res = await (window as any).go.app.App.RedisSetRemove(config, selectedKey, [member]);
                    if (res.success) {
                        message.success('删除成功');
                        loadKeyValue(selectedKey);
                    } else {
                        message.error('删除失败: ' + res.message);
                    }
                } catch (e: any) {
                    message.error('删除失败: ' + (e?.message || String(e)));
                }
            };

            return (
                <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
                    <div style={{ marginBottom: 8, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <Button size="small" style={actionButtonStyle} icon={<PlusOutlined />} onClick={() => {
                            Modal.confirm({
                                title: '添加成员',
                                content: (
                                    <Input.TextArea id="new-set-member" rows={4} placeholder="输入新成员值" />
                                ),
                                onOk: async () => {
                                    const member = (document.getElementById('new-set-member') as HTMLTextAreaElement)?.value;
                                    if (member) {
                                        await handleAddSetMember(member);
                                    }
                                }
                            });
                        }}>添加成员</Button>
                    </div>
                    <Table
                        dataSource={data}
                        columns={[
                            {
                                title: '成员',
                                dataIndex: 'displayValue',
                                key: 'member',
                                ellipsis: true,
                                render: (text: string, record: any) => {
                                    const tooltipContent = record.encoding && record.encoding !== 'UTF-8'
                                        ? `[${record.encoding}]\n${text}`
                                        : text;

                                    return (
                                        <Tooltip title={<pre style={{ maxHeight: 300, overflow: 'auto', margin: 0, fontSize: 12 }}>{tooltipContent}</pre>} styles={{ root: { maxWidth: 600 } }}>
                                            <span style={{
                                                color: record.isBinary ? '#d46b08' : (record.isJson ? jsonAccentColor : undefined),
                                                fontFamily: record.isBinary ? 'monospace' : undefined,
                                                fontSize: record.isBinary ? 11 : undefined
                                            }}>
                                                {text}
                                            </span>
                                        </Tooltip>
                                    );
                                }
                            },
                            {
                                title: '操作',
                                key: 'action',
                                width: 80,
                                render: (_: any, record: any) => (
                                    <Space size="small">
                                        <Tooltip title="复制值">
                                            <Button type="text" size="small" icon={<CopyOutlined />} onClick={() => {
                                                navigator.clipboard.writeText(record.member).then(() => {
                                                    message.success('已复制');
                                                }).catch(() => {
                                                    message.error('复制失败');
                                                });
                                            }} />
                                        </Tooltip>
                                        <Popconfirm title="确定删除此成员？" onConfirm={() => handleRemoveSetMember(record.member)}>
                                            <Button type="text" size="small" danger icon={<DeleteOutlined />} />
                                        </Popconfirm>
                                    </Space>
                                )
                            }
                        ]}
                        rowKey="index"
                        size="small"
                        pagination={{ pageSize: 50 }}
                        scroll={{ y: 'calc(100vh - 350px)' }}
                        style={{ flex: 1 }}
                    />
                </div>
            );
        };

        const renderZSetValue = () => {
            // 根据查看模式处理值
            const processValue = (value: string) => {
                if (viewMode === 'hex') {
                    return { displayValue: toHexDisplay(value), isBinary: true, isJson: false, encoding: 'HEX' };
                } else if (viewMode === 'text') {
                    return { displayValue: value, isBinary: false, isJson: false, encoding: 'Text' };
                } else if (viewMode === 'utf8') {
                    try {
                        const bytes = new Uint8Array(value.length);
                        for (let i = 0; i < value.length; i++) {
                            bytes[i] = value.charCodeAt(i) & 0xFF;
                        }
                        const decoded = new TextDecoder('utf-8', { fatal: false }).decode(bytes);
                        return { displayValue: decoded, isBinary: false, isJson: false, encoding: 'UTF-8' };
                    } catch (e) {
                        return { displayValue: value, isBinary: false, isJson: false, encoding: 'UTF-8 (失败)' };
                    }
                } else {
                    // auto mode
                    return formatStringValue(value);
                }
            };

            const data = (keyValue.value as Array<{ member: string; score: number }>).map((item, index) => {
                const { displayValue, isBinary, isJson, encoding } = processValue(item.member);
                return { ...item, index, displayMember: displayValue, isBinary, isJson, encoding };
            });

            const handleAddZSetMember = async (member: string, score: number) => {
                const config = getConfig();
                if (!config) return;
                try {
                    const res = await (window as any).go.app.App.RedisZSetAdd(config, selectedKey, [{ member, score }]);
                    if (res.success) {
                        message.success('添加成功');
                        loadKeyValue(selectedKey);
                    } else {
                        message.error('添加失败: ' + res.message);
                    }
                } catch (e: any) {
                    message.error('添加失败: ' + (e?.message || String(e)));
                }
            };

            const handleRemoveZSetMember = async (member: string) => {
                const config = getConfig();
                if (!config) return;
                try {
                    const res = await (window as any).go.app.App.RedisZSetRemove(config, selectedKey, [member]);
                    if (res.success) {
                        message.success('删除成功');
                        loadKeyValue(selectedKey);
                    } else {
                        message.error('删除失败: ' + res.message);
                    }
                } catch (e: any) {
                    message.error('删除失败: ' + (e?.message || String(e)));
                }
            };

            return (
                <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
                    <div style={{ marginBottom: 8, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <Button size="small" style={actionButtonStyle} icon={<PlusOutlined />} onClick={() => {
                            Modal.confirm({
                                title: '添加成员',
                                content: (
                                    <div>
                                        <div style={{ marginBottom: 8 }}>
                                            <label>分数：</label>
                                            <InputNumber id="new-zset-score" defaultValue={0} style={{ width: '100%' }} />
                                        </div>
                                        <div>
                                            <label>成员：</label>
                                            <Input.TextArea id="new-zset-member" rows={4} placeholder="输入成员值" />
                                        </div>
                                    </div>
                                ),
                                onOk: async () => {
                                    const score = parseFloat((document.getElementById('new-zset-score') as HTMLInputElement)?.value || '0');
                                    const member = (document.getElementById('new-zset-member') as HTMLTextAreaElement)?.value;
                                    if (member) {
                                        await handleAddZSetMember(member, score);
                                    }
                                }
                            });
                        }}>添加成员</Button>
                    </div>
                    <Table
                        dataSource={data}
                        columns={[
                            { title: '分数', dataIndex: 'score', key: 'score', width: 120 },
                            {
                                title: '成员',
                                dataIndex: 'displayMember',
                                key: 'member',
                                ellipsis: true,
                                render: (text: string, record: any) => {
                                    const tooltipContent = record.encoding && record.encoding !== 'UTF-8'
                                        ? `[${record.encoding}]\n${text}`
                                        : text;

                                    return (
                                        <Tooltip title={<pre style={{ maxHeight: 300, overflow: 'auto', margin: 0, fontSize: 12 }}>{tooltipContent}</pre>} styles={{ root: { maxWidth: 600 } }}>
                                            <span style={{
                                                color: record.isBinary ? '#d46b08' : (record.isJson ? jsonAccentColor : undefined),
                                                fontFamily: record.isBinary ? 'monospace' : undefined,
                                                fontSize: record.isBinary ? 11 : undefined
                                            }}>
                                                {text}
                                            </span>
                                        </Tooltip>
                                    );
                                }
                            },
                            {
                                title: '操作',
                                key: 'action',
                                width: 120,
                                render: (_: any, record: any) => (
                                    <Space size="small">
                                        <Tooltip title="复制值">
                                            <Button type="text" size="small" icon={<CopyOutlined />} onClick={() => {
                                                navigator.clipboard.writeText(record.member).then(() => {
                                                    message.success('已复制');
                                                }).catch(() => {
                                                    message.error('复制失败');
                                                });
                                            }} />
                                        </Tooltip>
                                        {!record.isBinary && (
                                            <Button type="text" size="small" icon={<EditOutlined />} onClick={() => {
                                                Modal.confirm({
                                                    title: '修改分数',
                                                    content: (
                                                        <div>
                                                            <label>新分数：</label>
                                                            <InputNumber id="edit-zset-score" defaultValue={record.score} style={{ width: '100%' }} />
                                                        </div>
                                                    ),
                                                    onOk: async () => {
                                                        const newScore = parseFloat((document.getElementById('edit-zset-score') as HTMLInputElement)?.value || '0');
                                                        await handleAddZSetMember(record.member, newScore);
                                                    }
                                                });
                                            }} />
                                        )}
                                        <Popconfirm title="确定删除此成员？" onConfirm={() => handleRemoveZSetMember(record.member)}>
                                            <Button type="text" size="small" danger icon={<DeleteOutlined />} />
                                        </Popconfirm>
                                    </Space>
                                )
                            }
                        ]}
                        rowKey="index"
                        size="small"
                        pagination={{ pageSize: 50 }}
                        scroll={{ y: 'calc(100vh - 350px)' }}
                        style={{ flex: 1 }}
                    />
                </div>
            );
        };

        const renderStreamValue = () => {
            const processValue = (value: string) => {
                if (viewMode === 'hex') {
                    return { displayValue: toHexDisplay(value), isBinary: true, isJson: false, encoding: 'HEX' };
                } else if (viewMode === 'text') {
                    return { displayValue: value, isBinary: false, isJson: false, encoding: 'Text' };
                } else if (viewMode === 'utf8') {
                    try {
                        const bytes = new Uint8Array(value.length);
                        for (let i = 0; i < value.length; i++) {
                            bytes[i] = value.charCodeAt(i) & 0xFF;
                        }
                        const decoded = new TextDecoder('utf-8', { fatal: false }).decode(bytes);
                        return { displayValue: decoded, isBinary: false, isJson: false, encoding: 'UTF-8' };
                    } catch (e) {
                        return { displayValue: value, isBinary: false, isJson: false, encoding: 'UTF-8 (失败)' };
                    }
                } else {
                    return formatStringValue(value);
                }
            };

            const data = (keyValue.value as StreamEntry[]).map((item, index) => {
                const rawFieldsText = JSON.stringify(item.fields ?? {}, null, 2);
                const { displayValue, isBinary, isJson, encoding } = processValue(rawFieldsText);
                return {
                    index,
                    id: item.id,
                    rawFieldsText,
                    displayFields: displayValue,
                    isBinary,
                    isJson,
                    encoding,
                };
            });

            const handleAddStreamEntry = async (fieldsText: string, id: string) => {
                const config = getConfig();
                if (!config) return;

                let parsed: unknown;
                try {
                    parsed = JSON.parse(fieldsText);
                } catch (e) {
                    message.error('字段 JSON 格式不正确');
                    return;
                }

                if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
                    message.error('字段必须是 JSON 对象');
                    return;
                }

                const fieldMap: Record<string, string> = {};
                Object.entries(parsed as Record<string, unknown>).forEach(([field, value]) => {
                    fieldMap[field] = value == null ? '' : String(value);
                });

                if (Object.keys(fieldMap).length === 0) {
                    message.error('至少提供一个字段');
                    return;
                }

                try {
                    const res = await (window as any).go.app.App.RedisStreamAdd(config, selectedKey, fieldMap, id || '*');
                    if (res.success) {
                        const newID = res.data?.id ? ` (${res.data.id})` : '';
                        message.success(`添加成功${newID}`);
                        loadKeyValue(selectedKey);
                    } else {
                        message.error('添加失败: ' + res.message);
                    }
                } catch (e: any) {
                    message.error('添加失败: ' + (e?.message || String(e)));
                }
            };

            const handleDeleteStreamEntry = async (id: string) => {
                const config = getConfig();
                if (!config) return;

                try {
                    const res = await (window as any).go.app.App.RedisStreamDelete(config, selectedKey, [id]);
                    if (res.success) {
                        const deleted = Number(res.data?.deleted ?? 0);
                        if (deleted > 0) {
                            message.success('删除成功');
                        } else {
                            message.warning('未删除任何消息，可能已不存在');
                        }
                        loadKeyValue(selectedKey);
                    } else {
                        message.error('删除失败: ' + res.message);
                    }
                } catch (e: any) {
                    message.error('删除失败: ' + (e?.message || String(e)));
                }
            };

            return (
                <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
                    <div style={{ marginBottom: 8, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <Button size="small" style={actionButtonStyle} icon={<PlusOutlined />} onClick={() => {
                            Modal.confirm({
                                title: '添加 Stream 消息',
                                width: 680,
                                content: (
                                    <div>
                                        <div style={{ marginBottom: 8 }}>
                                            <label>ID（可选，默认 *）：</label>
                                            <Input id="new-stream-id" placeholder="例如: * 或 1723110000000-0" />
                                        </div>
                                        <div>
                                            <label>字段 JSON：</label>
                                            <Input.TextArea id="new-stream-fields" rows={8} defaultValue={'{\n  "field": "value"\n}'} />
                                        </div>
                                    </div>
                                ),
                                onOk: async () => {
                                    const id = (document.getElementById('new-stream-id') as HTMLInputElement)?.value?.trim() || '*';
                                    const fieldsText = (document.getElementById('new-stream-fields') as HTMLTextAreaElement)?.value || '{}';
                                    await handleAddStreamEntry(fieldsText, id);
                                }
                            });
                        }}>添加消息</Button>
                    </div>
                    <Table
                        dataSource={data}
                        columns={[
                            {
                                title: 'ID',
                                dataIndex: 'id',
                                key: 'id',
                                width: 240,
                                ellipsis: true,
                            },
                            {
                                title: '字段',
                                dataIndex: 'displayFields',
                                key: 'fields',
                                ellipsis: true,
                                render: (text: string, record: any) => {
                                    const tooltipContent = record.encoding && record.encoding !== 'UTF-8'
                                        ? `[${record.encoding}]\n${text}`
                                        : text;

                                    return (
                                        <Tooltip title={<pre style={{ maxHeight: 300, overflow: 'auto', margin: 0, fontSize: 12 }}>{tooltipContent}</pre>} styles={{ root: { maxWidth: 720 } }}>
                                            <span style={{
                                                color: record.isBinary ? '#d46b08' : (record.isJson ? jsonAccentColor : undefined),
                                                fontFamily: record.isBinary ? 'monospace' : undefined,
                                                fontSize: record.isBinary ? 11 : undefined
                                            }}>
                                                {text}
                                            </span>
                                        </Tooltip>
                                    );
                                }
                            },
                            {
                                title: '操作',
                                key: 'action',
                                width: 140,
                                render: (_: any, record: any) => (
                                    <Space size="small">
                                        <Tooltip title="复制 ID">
                                            <Button type="text" size="small" icon={<CopyOutlined />} onClick={() => {
                                                navigator.clipboard.writeText(record.id).then(() => {
                                                    message.success('已复制');
                                                }).catch(() => {
                                                    message.error('复制失败');
                                                });
                                            }} />
                                        </Tooltip>
                                        <Tooltip title="复制字段 JSON">
                                            <Button type="text" size="small" icon={<CopyOutlined />} onClick={() => {
                                                navigator.clipboard.writeText(record.rawFieldsText).then(() => {
                                                    message.success('已复制');
                                                }).catch(() => {
                                                    message.error('复制失败');
                                                });
                                            }} />
                                        </Tooltip>
                                        <Popconfirm title="确定删除此消息？" onConfirm={() => handleDeleteStreamEntry(record.id)}>
                                            <Button type="text" size="small" danger icon={<DeleteOutlined />} />
                                        </Popconfirm>
                                    </Space>
                                )
                            }
                        ]}
                        rowKey="id"
                        size="small"
                        pagination={{ pageSize: 50 }}
                        scroll={{ y: 'calc(100vh - 350px)' }}
                        style={{ flex: 1 }}
                    />
                </div>
            );
        };

        return (
            <div style={{ height: '100%', display: 'flex', flexDirection: 'column', gap: 12 }}>
                <div style={{ ...workbenchCardStyle, padding: 18, display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 16, flexShrink: 0 }}>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 10, minWidth: 0 }}>
                        <span style={{ fontSize: 12, textTransform: 'uppercase', letterSpacing: '.08em', color: workbenchTheme.textMuted, fontWeight: 600 }}>
                            Active Key
                        </span>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap', minWidth: 0 }}>
                            <Tooltip title={selectedKey}>
                                <strong style={{ maxWidth: 340, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', fontSize: 26, color: workbenchTheme.textPrimary }}>
                                    {selectedKey}
                                </strong>
                            </Tooltip>
                            <Tooltip title="复制 Key 名称">
                                <Button
                                    type="text"
                                    size="small"
                                    icon={<CopyOutlined />}
                                    style={{ padding: '0 4px', display: 'flex', alignItems: 'center', color: workbenchTheme.textMuted }}
                                    onClick={() => {
                                        navigator.clipboard.writeText(selectedKey).then(() => {
                                            message.success('已复制 Key 名称');
                                        }).catch(() => {
                                            message.error('复制失败');
                                        });
                                    }}
                                />
                            </Tooltip>
                            <Tag color={getTypeColor(keyValue.type)} style={pillTagStyle}>{keyValue.type}</Tag>
                            <Tag icon={<ClockCircleOutlined />} style={mutedPillTagStyle}>{formatTTL(keyValue.ttl)}</Tag>
                            {keyValue.length > 0 && <Tag style={mutedPillTagStyle}>长度: {keyValue.length}</Tag>}
                        </div>
                    </div>
                    <div style={{ ...workbenchSubCardStyle, padding: 4, display: 'flex', gap: 4, alignItems: 'center', flexWrap: 'wrap', justifyContent: 'flex-end' }}>
                        <Button size="small" style={actionButtonStyle} onClick={() => {
                            ttlForm.setFieldsValue({ ttl: keyValue.ttl > 0 ? keyValue.ttl : -1 });
                            setTtlModalOpen(true);
                        }}>设置 TTL</Button>
                        <Button size="small" style={actionButtonStyle} onClick={() => loadKeyValue(selectedKey)} icon={<ReloadOutlined />}>刷新</Button>
                        <Popconfirm title={`确定删除 Key "${selectedKey}"？`} onConfirm={handleDeleteCurrentKey}>
                            <Button size="small" style={dangerActionButtonStyle} icon={<DeleteOutlined />}>删除 Key</Button>
                        </Popconfirm>
                    </div>
                </div>
                <div style={{ ...workbenchSubCardStyle, padding: 6, display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12, flexShrink: 0 }}>
                    <span style={{ paddingInline: 10, fontSize: 12, color: workbenchTheme.textMuted }}>查看模式</span>
                    <Radio.Group size="small" value={viewMode} onChange={(e) => setViewMode(e.target.value)}>
                        <Radio.Button value="auto">自动</Radio.Button>
                        <Radio.Button value="text">原始文本</Radio.Button>
                        <Radio.Button value="utf8">UTF-8</Radio.Button>
                        <Radio.Button value="hex">十六进制</Radio.Button>
                    </Radio.Group>
                </div>
                <div style={{ ...workbenchCardStyle, padding: 14, flex: 1, minHeight: 0, overflow: 'hidden' }}>
                    <div style={{ flex: 1, minHeight: 0, overflow: 'hidden', height: '100%' }}>
                        {keyValue.type === 'string' && renderStringValue()}
                        {keyValue.type === 'hash' && renderHashValue()}
                        {keyValue.type === 'list' && renderListValue()}
                        {keyValue.type === 'set' && renderSetValue()}
                        {keyValue.type === 'zset' && renderZSetValue()}
                        {keyValue.type === 'stream' && renderStreamValue()}
                    </div>
                </div>
            </div>
        );
    };

    if (!connection) {
        return <div style={{ padding: 20 }}>连接不存在</div>;
    }

    return (
        <div className="redis-viewer-workbench" style={{ display: 'flex', height: '100%', gap: 12, padding: 12, background: workbenchTheme.appBg, backdropFilter: blurToFilter(blur), WebkitBackdropFilter: blurToFilter(blur) }}>
            {/* Left: Key List */}
            <div ref={leftPanelRef} style={{ width: leftPanelWidth, minWidth: 300, display: 'flex', flexDirection: 'column', flexShrink: 0, gap: 12 }}>
                <div style={{ ...workbenchCardStyle, padding: 12 }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 12, marginBottom: 12 }}>
                        <div>
                            <div style={{ fontSize: 12, textTransform: 'uppercase', letterSpacing: '.08em', color: workbenchTheme.textMuted, fontWeight: 600 }}>Key Explorer</div>
                            <div style={{ fontSize: 24, fontWeight: 700, color: workbenchTheme.textPrimary, marginTop: 4 }}>db{redisDB}</div>
                        </div>
                        <Tag style={mutedPillTagStyle}>{keys.length} Keys</Tag>
                    </div>
                    <Space.Compact style={{ width: '100%' }}>
                        <Search
                            placeholder="搜索 Key (支持 * 通配符)"
                            defaultValue="*"
                            onSearch={handleSearch}
                            enterButton={<SearchOutlined />}
                        />
                    </Space.Compact>
                    <div style={{ marginTop: 12, display: 'flex', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap' }}>
                        <Space wrap size={8}>
                            <Button size="small" style={actionButtonStyle} icon={<ReloadOutlined />} onClick={handleRefresh}>刷新</Button>
                            <Button size="small" style={actionButtonStyle} icon={<PlusOutlined />} onClick={() => setNewKeyModalOpen(true)}>新建</Button>
                            <Button size="small" style={primaryActionButtonStyle} onClick={handleSelectAllLoadedKeys} disabled={keys.length === 0}>全选全部</Button>
                            <Button size="small" style={actionButtonStyle} onClick={handleClearAllSelectedKeys} disabled={selectedKeys.length === 0}>取消全选</Button>
                        </Space>
                        <Popconfirm
                            title={`确定删除选中的 ${selectedKeys.length} 个 Key？`}
                            onConfirm={() => handleDeleteKeys(selectedKeys)}
                            disabled={selectedKeys.length === 0}
                        >
                            <Button size="small" style={dangerActionButtonStyle} icon={<DeleteOutlined />} disabled={selectedKeys.length === 0}>
                                删除选中({selectedKeys.length})
                            </Button>
                        </Popconfirm>
                    </div>
                </div>
                <div style={{ ...workbenchCardStyle, flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column', padding: 10 }}>
                    {isLargeKeyspace && (
                        <div style={{ padding: '8px 10px', fontSize: 12, color: workbenchTheme.textMuted, marginBottom: 8, borderRadius: 12, background: workbenchTheme.panelBgSubtle, border: workbenchTheme.panelBorder }}>
                            已启用大数据量性能模式（简化节点渲染，最多保留 {REDIS_LARGE_KEYSPACE_MAX_EXPANDED_GROUPS} 个展开分组）
                        </div>
                    )}
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '4px 8px 10px 8px', color: workbenchTheme.textMuted, fontSize: 12, textTransform: 'uppercase', letterSpacing: '.06em' }}>
                        <span>命名空间 / Key</span>
                        <span>类型 / TTL</span>
                    </div>
                    <div ref={treeContainerRef} style={{ ...workbenchSubCardStyle, flex: 1, minHeight: 0, overflow: 'hidden', padding: 6 }}>
                        <Spin spinning={loading} size="small" style={{ width: '100%' }}>
                            <Tree
                                blockNode
                                showIcon={false}
                                switcherIcon={() => null}
                                checkable
                                checkStrictly
                                selectable
                                virtual
                                height={Math.max(treeHeight - 8, 220)}
                                treeData={keyTree.treeData}
                                titleRender={renderTreeNodeTitle}
                                selectedKeys={selectedTreeNodeKeys}
                                checkedKeys={checkedTreeNodeKeys}
                                expandedKeys={expandedGroupKeys}
                                onExpand={handleTreeExpand}
                                onSelect={(nodeKeys) => handleTreeSelect(nodeKeys)}
                                onCheck={(checked, info) => handleTreeCheck(checked, info)}
                                onRightClick={handleTreeRightClick}
                                style={{ padding: '8px 6px' }}
                            />
                        </Spin>
                    </div>
                    {hasMore && (
                        <div style={{ padding: 10, textAlign: 'center' }}>
                            <Button style={actionButtonStyle} onClick={handleLoadMore} loading={loading} disabled={!hasMore || loading}>加载更多</Button>
                        </div>
                    )}
                </div>
            </div>

            {/* Resizable Divider */}
            <ResizableDivider targetRef={leftPanelRef} onResizeEnd={setLeftPanelWidth} />

            {/* Right: Value Viewer */}
            <div style={{ flex: 1, overflow: 'hidden', minWidth: 300 }}>
                {valueLoading ? (
                    <div style={{ ...workbenchCardStyle, padding: 20, textAlign: 'center', color: workbenchTheme.textMuted }}>加载中...</div>
                ) : (
                    renderValueEditor()
                )}
            </div>

            {/* Edit String Modal */}
            <Modal
                title="编辑值"
                open={editModalOpen}
                onOk={handleSaveString}
                onCancel={() => setEditModalOpen(false)}
                width={800}
                styles={{ content: redisModalContentStyle, header: { background: 'transparent', borderBottom: 'none', color: workbenchTheme.textPrimary }, body: { height: 500, paddingTop: 8 }, footer: { background: 'transparent', borderTop: 'none' } }}
            >
                <Editor
                    height="450px"
                    language={tryFormatJson(editValue).isJson ? 'json' : 'plaintext'}
                    theme={darkMode ? 'transparent-dark' : 'transparent-light'}
                    value={editValue}
                    onChange={(value) => setEditValue(value || '')}
                    options={{
                        minimap: { enabled: false },
                        lineNumbers: 'on',
                        wordWrap: 'on',
                        scrollBeyondLastLine: false,
                        automaticLayout: true,
                        folding: true
                    }}
                />
            </Modal>

            {/* New Key Modal */}
            <Modal
                title="新建 Key"
                open={newKeyModalOpen}
                onOk={handleCreateKey}
                onCancel={() => setNewKeyModalOpen(false)}
                styles={{ content: redisModalContentStyle, header: { background: 'transparent', borderBottom: 'none', color: workbenchTheme.textPrimary }, body: { paddingTop: 8 }, footer: { background: 'transparent', borderTop: 'none' } }}
            >
                <Form form={newKeyForm} layout="vertical" initialValues={{ ttl: -1 }}>
                    <Form.Item name="key" label="Key" rules={[{ required: true, message: '请输入 Key' }]}>
                        <Input placeholder="key name" />
                    </Form.Item>
                    <Form.Item name="value" label="值" rules={[{ required: true, message: '请输入值' }]}>
                        <Input.TextArea rows={4} placeholder="value" />
                    </Form.Item>
                    <Form.Item name="ttl" label="TTL (秒)" help="-1 表示永不过期">
                        <InputNumber style={{ width: '100%' }} min={-1} />
                    </Form.Item>
                </Form>
            </Modal>

            {/* TTL Modal */}
            <Modal
                title="重命名 Key"
                open={renameKeyModalOpen}
                onOk={handleRenameKey}
                onCancel={() => {
                    setRenameKeyModalOpen(false);
                    setRenameTargetKey(null);
                    renameKeyForm.resetFields();
                }}
                styles={{ content: redisModalContentStyle, header: { background: 'transparent', borderBottom: 'none', color: workbenchTheme.textPrimary }, body: { paddingTop: 8 }, footer: { background: 'transparent', borderTop: 'none' } }}
            >
                <Form form={renameKeyForm} layout="vertical">
                    <Form.Item
                        name="key"
                        label="新的 Key 名称"
                        rules={[{ required: true, message: '请输入新的 Key 名称' }]}
                        extra={renameTargetKey ? `原始 Key：${renameTargetKey}` : undefined}
                    >
                        <Input placeholder="new:key:name" />
                    </Form.Item>
                </Form>
            </Modal>

            <Modal
                title="设置 TTL"
                open={ttlModalOpen}
                onOk={handleSetTTL}
                onCancel={() => setTtlModalOpen(false)}
                styles={{ content: redisModalContentStyle, header: { background: 'transparent', borderBottom: 'none', color: workbenchTheme.textPrimary }, body: { paddingTop: 8 }, footer: { background: 'transparent', borderTop: 'none' } }}
            >
                <Form form={ttlForm} layout="vertical">
                    <Form.Item name="ttl" label="TTL (秒)" help="-1 表示永不过期">
                        <InputNumber style={{ width: '100%' }} min={-1} />
                    </Form.Item>
                </Form>
            </Modal>

            {/* JSON Edit Modal with Monaco Editor */}
            <Modal
                title={jsonEditConfig?.title || '编辑'}
                open={jsonEditModalOpen}
                onOk={async () => {
                    if (jsonEditConfig?.onSave) {
                        await jsonEditConfig.onSave(jsonEditValueRef.current);
                    }
                    setJsonEditModalOpen(false);
                }}
                onCancel={() => setJsonEditModalOpen(false)}
                width={800}
                styles={{ content: redisModalContentStyle, header: { background: 'transparent', borderBottom: 'none', color: workbenchTheme.textPrimary }, body: { height: 500, paddingTop: 8 }, footer: { background: 'transparent', borderTop: 'none' } }}
            >
                <Editor
                    height="450px"
                    language={jsonEditConfig?.isJson ? 'json' : 'plaintext'}
                    theme={darkMode ? 'transparent-dark' : 'transparent-light'}
                    defaultValue={jsonEditConfig?.value || ''}
                    onChange={(value) => { jsonEditValueRef.current = value || ''; }}
                    onMount={(editor) => { jsonEditValueRef.current = jsonEditConfig?.value || ''; }}
                    options={{
                        minimap: { enabled: false },
                        lineNumbers: 'on',
                        wordWrap: 'on',
                        scrollBeyondLastLine: false,
                        automaticLayout: true,
                        folding: true,
                        formatOnPaste: true
                    }}
                />
            </Modal>
            {treeContextMenu && typeof document !== 'undefined' && createPortal((
                <div
                    style={{
                        position: 'fixed',
                        left: typeof window !== 'undefined' ? Math.min(treeContextMenu.x + 4, Math.max(16, window.innerWidth - 220)) : treeContextMenu.x,
                        top: typeof window !== 'undefined' ? Math.min(treeContextMenu.y + 4, Math.max(16, window.innerHeight - 140)) : treeContextMenu.y,
                        zIndex: 1200,
                        minWidth: 188,
                        padding: 8,
                        borderRadius: 14,
                        background: workbenchTheme.panelBgStrong,
                        border: workbenchTheme.panelBorder,
                        boxShadow: `${workbenchTheme.panelInset}, ${workbenchTheme.shadow}`,
                        backdropFilter: workbenchTheme.backdropFilter,
                        WebkitBackdropFilter: workbenchTheme.backdropFilter,
                    }}
                    onClick={(event) => event.stopPropagation()}
                >
                    <Button
                        type="text"
                        style={{ width: '100%', justifyContent: 'flex-start', height: 40, borderRadius: 10, color: workbenchTheme.textPrimary, fontWeight: 600 }}
                        icon={<EditOutlined />}
                        onClick={() => openRenameKeyModal(treeContextMenu.rawKey)}
                    >
                        重命名 Key
                    </Button>
                    <Button
                        type="text"
                        style={{ width: '100%', justifyContent: 'flex-start', height: 40, borderRadius: 10, color: workbenchTheme.textPrimary, fontWeight: 600 }}
                        icon={<CopyOutlined />}
                        onClick={async () => {
                            try {
                                await navigator.clipboard.writeText(treeContextMenu.rawKey);
                                setTreeContextMenu(null);
                                message.success('已复制 Key 名称');
                            } catch {
                                message.error('复制失败');
                            }
                        }}
                    >
                        复制 Key 名称
                    </Button>
                </div>
            ), document.body)}
        </div>
    );
};

export default RedisViewer;
