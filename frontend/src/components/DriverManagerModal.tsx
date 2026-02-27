import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { Button, Modal, Progress, Select, Space, Table, Tag, Typography, message } from 'antd';
import { DeleteOutlined, DownloadOutlined, ReloadOutlined } from '@ant-design/icons';
import { EventsOn } from '../../wailsjs/runtime/runtime';
import {
  DownloadDriverPackage,
  GetDriverVersionList,
  GetDriverVersionPackageSize,
  GetDriverStatusList,
  RemoveDriverPackage,
} from '../../wailsjs/go/app/App';

const { Text } = Typography;

type DriverStatusRow = {
  type: string;
  name: string;
  builtIn: boolean;
  pinnedVersion?: string;
  installedVersion?: string;
  packageSizeText?: string;
  runtimeAvailable: boolean;
  packageInstalled: boolean;
  connectable: boolean;
  defaultDownloadUrl?: string;
  message?: string;
};

type DriverProgressEvent = {
  driverType?: string;
  status?: 'start' | 'downloading' | 'done' | 'error';
  message?: string;
  percent?: number;
};

type ProgressState = {
  status: 'start' | 'downloading' | 'done' | 'error';
  message: string;
  percent: number;
};

type DriverVersionOption = {
  version: string;
  downloadUrl: string;
  packageSizeText?: string;
  recommended?: boolean;
  source?: string;
  year?: string;
  displayLabel?: string;
};

const buildVersionOptionKey = (option: DriverVersionOption) => `${option.version}@@${option.downloadUrl}`;
const buildVersionSizeLoadingKey = (driverType: string, optionKey: string) => `${driverType}@@${optionKey}`;

const buildVersionSelectOptions = (options: DriverVersionOption[]) => {
  type SelectOption = { value: string; label: string };
  type SelectGroup = { label: string; options: SelectOption[] };

  if (options.length === 0) {
    return [] as Array<SelectOption | SelectGroup>;
  }

  const yearGroups = new Map<string, SelectOption[]>();
  const others: SelectOption[] = [];
  options.forEach((option) => {
    const selectOption: SelectOption = {
      value: buildVersionOptionKey(option),
      label: option.displayLabel || option.version || '默认版本',
    };
    const year = String(option.year || '').trim();
    if (!year) {
      others.push(selectOption);
      return;
    }
    const group = yearGroups.get(year) || [];
    group.push(selectOption);
    yearGroups.set(year, group);
  });

  const sortedYears = Array.from(yearGroups.keys()).sort((a, b) => {
    const left = Number.parseInt(a, 10);
    const right = Number.parseInt(b, 10);
    const leftValid = Number.isFinite(left);
    const rightValid = Number.isFinite(right);
    if (leftValid && rightValid) {
      return right - left;
    }
    return b.localeCompare(a);
  });

  const grouped: SelectGroup[] = sortedYears.map((year) => ({
    label: `${year} 年`,
    options: yearGroups.get(year) || [],
  }));
  if (others.length > 0) {
    grouped.push({ label: '其他', options: others });
  }
  return grouped;
};

const DriverManagerModal: React.FC<{ open: boolean; onClose: () => void }> = ({ open, onClose }) => {
  const [loading, setLoading] = useState(false);
  const [downloadDir, setDownloadDir] = useState('');
  const [rows, setRows] = useState<DriverStatusRow[]>([]);
  const [actionDriver, setActionDriver] = useState('');
  const [progressMap, setProgressMap] = useState<Record<string, ProgressState>>({});
  const [versionMap, setVersionMap] = useState<Record<string, DriverVersionOption[]>>({});
  const [selectedVersionMap, setSelectedVersionMap] = useState<Record<string, string>>({});
  const [versionLoadingMap, setVersionLoadingMap] = useState<Record<string, boolean>>({});
  const [versionSizeLoadingMap, setVersionSizeLoadingMap] = useState<Record<string, boolean>>({});

  const refreshStatus = useCallback(async (toastOnError = true) => {
    setLoading(true);
    try {
      const res = await GetDriverStatusList(downloadDir, '');
      if (!res?.success) {
        if (toastOnError) {
          message.error(res?.message || '拉取驱动状态失败');
        }
        return;
      }

      const data = (res?.data || {}) as any;
      const resolvedDir = String(data.downloadDir || '').trim();
      const drivers = Array.isArray(data.drivers) ? data.drivers : [];

      if (resolvedDir) {
        setDownloadDir(resolvedDir);
      }

      const nextRows: DriverStatusRow[] = drivers.map((item: any) => ({
        type: String(item.type || '').trim(),
        name: String(item.name || item.type || '').trim(),
        builtIn: !!item.builtIn,
        pinnedVersion: String(item.pinnedVersion || '').trim() || undefined,
        installedVersion: String(item.installedVersion || '').trim() || undefined,
        packageSizeText: String(item.packageSizeText || '').trim() || undefined,
        runtimeAvailable: !!item.runtimeAvailable,
        packageInstalled: !!item.packageInstalled,
        connectable: !!item.connectable,
        defaultDownloadUrl: String(item.defaultDownloadUrl || '').trim() || undefined,
        message: String(item.message || '').trim() || undefined,
      }));
      setRows(nextRows);
    } catch (err: any) {
      if (toastOnError) {
        message.error(`拉取驱动状态失败：${err?.message || String(err)}`);
      }
    } finally {
      setLoading(false);
    }
  }, [downloadDir]);

  const loadVersionOptions = useCallback(async (row: DriverStatusRow, toastOnError = false) => {
    if (row.builtIn) {
      return [] as DriverVersionOption[];
    }
    const driverType = String(row.type || '').trim();
    if (!driverType) {
      return [] as DriverVersionOption[];
    }
    setVersionLoadingMap((prev) => ({ ...prev, [driverType]: true }));
    try {
      const res = await GetDriverVersionList(driverType, '');
      if (!res?.success) {
        if (toastOnError) {
          message.error(res?.message || `${row.name} 版本列表加载失败`);
        }
        return [] as DriverVersionOption[];
      }
      const data = (res?.data || {}) as any;
      const rawVersions = Array.isArray(data.versions) ? data.versions : [];
      const options: DriverVersionOption[] = rawVersions
        .map((item: any) => {
          const version = String(item.version || '').trim();
          const downloadUrl = String(item.downloadUrl || '').trim();
          if (!version && !downloadUrl) {
            return null;
          }
          return {
            version,
            downloadUrl,
            packageSizeText: String(item.packageSizeText || '').trim() || undefined,
            recommended: !!item.recommended,
            source: String(item.source || '').trim() || undefined,
            year: String(item.year || '').trim() || undefined,
            displayLabel: String(item.displayLabel || '').trim() || undefined,
          } as DriverVersionOption;
        })
        .filter((item: DriverVersionOption | null): item is DriverVersionOption => !!item);

      if (options.length === 0) {
        const fallbackVersion = String(row.pinnedVersion || '').trim();
        const fallbackURL = String(row.defaultDownloadUrl || '').trim();
        if (fallbackVersion || fallbackURL) {
          options.push({
            version: fallbackVersion,
            downloadUrl: fallbackURL,
            recommended: true,
            source: 'fallback',
            displayLabel: fallbackVersion || '默认版本',
          });
        }
      }

      setVersionMap((prev) => ({ ...prev, [driverType]: options }));
      setSelectedVersionMap((prev) => {
        const currentKey = prev[driverType];
        if (currentKey && options.some((option) => buildVersionOptionKey(option) === currentKey)) {
          return prev;
        }
        const preferred =
          options.find((option) => option.version === row.installedVersion) ||
          options.find((option) => option.version === row.pinnedVersion) ||
          options.find((option) => option.recommended) ||
          options[0];
        if (!preferred) {
          return prev;
        }
        return { ...prev, [driverType]: buildVersionOptionKey(preferred) };
      });
      return options;
    } catch (err: any) {
      if (toastOnError) {
        message.error(`加载 ${row.name} 版本列表失败：${err?.message || String(err)}`);
      }
      return [] as DriverVersionOption[];
    } finally {
      setVersionLoadingMap((prev) => ({ ...prev, [driverType]: false }));
    }
  }, []);

  const loadVersionPackageSize = useCallback(async (row: DriverStatusRow, optionKey: string) => {
    if (row.builtIn) {
      return;
    }
    const driverType = String(row.type || '').trim();
    if (!driverType || !optionKey) {
      return;
    }

    const options = versionMap[driverType] || [];
    const selectedOption = options.find((item) => buildVersionOptionKey(item) === optionKey);
    if (!selectedOption) {
      return;
    }
    if (String(selectedOption.packageSizeText || '').trim()) {
      return;
    }

    const versionText = String(selectedOption.version || '').trim();
    if (!versionText) {
      return;
    }

    const loadingKey = buildVersionSizeLoadingKey(driverType, optionKey);
    if (versionSizeLoadingMap[loadingKey]) {
      return;
    }

    setVersionSizeLoadingMap((prev) => ({ ...prev, [loadingKey]: true }));
    try {
      const res = await GetDriverVersionPackageSize(driverType, versionText);
      if (!res?.success) {
        return;
      }
      const data = (res?.data || {}) as any;
      const sizeText = String(data.packageSizeText || '').trim();
      if (!sizeText) {
        return;
      }

      setVersionMap((prev) => {
        const current = prev[driverType] || [];
        let changed = false;
        const next = current.map((item) => {
          if (buildVersionOptionKey(item) !== optionKey) {
            return item;
          }
          if (String(item.packageSizeText || '').trim() === sizeText) {
            return item;
          }
          changed = true;
          return { ...item, packageSizeText: sizeText };
        });
        if (!changed) {
          return prev;
        }
        return { ...prev, [driverType]: next };
      });
    } finally {
      setVersionSizeLoadingMap((prev) => {
        if (!prev[loadingKey]) {
          return prev;
        }
        const next = { ...prev };
        delete next[loadingKey];
        return next;
      });
    }
  }, [versionMap, versionSizeLoadingMap]);

  useEffect(() => {
    if (!open) {
      return;
    }
    refreshStatus(false);
  }, [open, refreshStatus]);

  useEffect(() => {
    if (!open) {
      return;
    }
    const off = EventsOn('driver:download-progress', (event: DriverProgressEvent) => {
      if (!event) {
        return;
      }
      const driverType = String(event.driverType || '').trim().toLowerCase();
      const status = event.status;
      if (!driverType || !status) {
        return;
      }
      const messageText = String(event.message || '').trim();
      const percent = Math.max(0, Math.min(100, Number(event.percent || 0)));
      setProgressMap((prev) => ({
        ...prev,
        [driverType]: {
          status,
          message: messageText,
          percent,
        },
      }));
    });
    return () => {
      off();
    };
  }, [open]);

  const installDriver = useCallback(async (row: DriverStatusRow) => {
    setActionDriver(row.type);
    setProgressMap((prev) => ({
      ...prev,
      [row.type]: {
        status: 'start',
        message: '开始安装',
        percent: 0,
      },
    }));
    try {
      let options = versionMap[row.type] || [];
      if (options.length === 0) {
        options = await loadVersionOptions(row, true);
      }
      const selectedKey = selectedVersionMap[row.type];
      const selectedOption =
        options.find((item) => buildVersionOptionKey(item) === selectedKey) ||
        options.find((item) => item.recommended) ||
        options[0];
      const selectedVersion = selectedOption?.version || row.pinnedVersion || '';
      const selectedDownloadURL = selectedOption?.downloadUrl || row.defaultDownloadUrl || '';

      const result = await DownloadDriverPackage(row.type, selectedVersion, selectedDownloadURL, downloadDir);
      if (!result?.success) {
        message.error(result?.message || `安装 ${row.name} 失败`);
        return;
      }
      const versionTip = selectedVersion ? `（${selectedVersion}）` : '';
      message.success(`${row.name}${versionTip} 已安装启用`);
      refreshStatus(false);
    } finally {
      setActionDriver('');
    }
  }, [downloadDir, loadVersionOptions, refreshStatus, selectedVersionMap, versionMap]);

  const removeDriver = useCallback(async (row: DriverStatusRow) => {
    setActionDriver(row.type);
    try {
      const result = await RemoveDriverPackage(row.type, downloadDir);
      if (!result?.success) {
        message.error(result?.message || `移除 ${row.name} 失败`);
        return;
      }
      message.success(`${row.name} 已移除`);
      setProgressMap((prev) => {
        const next = { ...prev };
        delete next[row.type];
        return next;
      });
      refreshStatus(false);
    } finally {
      setActionDriver('');
    }
  }, [downloadDir, refreshStatus]);

  const columns = useMemo(() => {
    return [
      {
        title: '数据源',
        dataIndex: 'name',
        key: 'name',
        width: 150,
      },
      {
        title: '安装包大小',
        dataIndex: 'packageSizeText',
        key: 'packageSizeText',
        width: 120,
        render: (_: string | undefined, row: DriverStatusRow) => {
          if (row.builtIn) {
            return row.packageSizeText || '-';
          }
          const options = versionMap[row.type] || [];
          const selectedKey = selectedVersionMap[row.type];
          const loadingKey = buildVersionSizeLoadingKey(row.type, selectedKey || '');
          const selectedOption =
            options.find((item) => buildVersionOptionKey(item) === selectedKey) ||
            options.find((item) => item.recommended) ||
            options[0];
          const anyKnownSize = options.find((item) => String(item.packageSizeText || '').trim())?.packageSizeText;
          if (selectedKey && versionSizeLoadingMap[loadingKey]) {
            return '计算中...';
          }
          return selectedOption?.packageSizeText || anyKnownSize || row.packageSizeText || '-';
        },
      },
      {
        title: '状态',
        key: 'status',
        width: 140,
        render: (_: string, row: DriverStatusRow) => {
          if (row.builtIn) {
            return <Tag color="success">内置可用</Tag>;
          }
          const progress = progressMap[row.type];
          if (progress && (progress.status === 'start' || progress.status === 'downloading')) {
            return <Tag color="processing">安装中 {Math.round(progress.percent)}%</Tag>;
          }
          if (row.connectable) {
            return <Tag color="success">已启用</Tag>;
          }
          if (row.packageInstalled) {
            return <Tag color="warning">已安装</Tag>;
          }
          return <Tag color="default">未启用</Tag>;
        },
      },
      {
        title: '安装进度',
        key: 'progress',
        width: 170,
        render: (_: string, row: DriverStatusRow) => {
          if (row.builtIn) {
            return <Text type="secondary">-</Text>;
          }

          const progress = progressMap[row.type];
          let percent = 0;
          let status: 'normal' | 'exception' | 'active' | 'success' = 'normal';

          if (progress?.status === 'error') {
            percent = Math.max(0, Math.min(100, Math.round(progress.percent || 0)));
            status = 'exception';
          } else if (progress && (progress.status === 'start' || progress.status === 'downloading')) {
            percent = Math.max(1, Math.min(99, Math.round(progress.percent || 0)));
            status = 'active';
          } else if (row.connectable || row.packageInstalled) {
            percent = 100;
            status = 'success';
          }

          return <Progress percent={percent} status={status} size="small" />;
        },
      },
      {
        title: '驱动版本',
        key: 'driverVersion',
        width: 230,
        render: (_: string, row: DriverStatusRow) => {
          if (row.builtIn) {
            return <Text type="secondary">-</Text>;
          }
          const options = versionMap[row.type] || [];
          const selectedKey = selectedVersionMap[row.type];
          const selectOptions = buildVersionSelectOptions(options);
          return (
            <Select
              size="small"
              style={{ width: '100%' }}
              loading={!!versionLoadingMap[row.type]}
              disabled={actionDriver === row.type}
              placeholder={options.length > 0 ? '选择驱动版本' : '点击展开加载版本'}
              value={selectedKey}
              options={selectOptions as any}
              onOpenChange={(open) => {
                if (open && options.length === 0 && !versionLoadingMap[row.type]) {
                  void loadVersionOptions(row, true);
                  return;
                }
                if (open && selectedKey) {
                  void loadVersionPackageSize(row, selectedKey);
                }
              }}
              onChange={(value) => {
                setSelectedVersionMap((prev) => ({ ...prev, [row.type]: value }));
                void loadVersionPackageSize(row, value);
              }}
            />
          );
        },
      },
      {
        title: '操作',
        key: 'actions',
        width: 190,
        render: (_: string, row: DriverStatusRow) => {
          if (row.builtIn) {
            return <Text type="secondary">-</Text>;
          }
          const isSlimBuildUnavailable = (row.message || '').includes('精简构建');
          const loadingAction = actionDriver === row.type;
          if (isSlimBuildUnavailable && !row.packageInstalled) {
            return <Text type="secondary">需 Full 版</Text>;
          }
          if (row.connectable) {
            return (
              <Button
                danger
                icon={<DeleteOutlined />}
                loading={loadingAction}
                onClick={() => removeDriver(row)}
              >
                移除
              </Button>
            );
          }
          return (
            <Button
              type="primary"
              icon={<DownloadOutlined />}
              loading={loadingAction}
              onClick={() => installDriver(row)}
            >
              安装启用
            </Button>
          );
        },
      },
    ];
  }, [actionDriver, installDriver, loadVersionOptions, loadVersionPackageSize, progressMap, removeDriver, selectedVersionMap, versionLoadingMap, versionMap, versionSizeLoadingMap]);

  return (
    <Modal
      title="驱动管理"
      open={open}
      onCancel={onClose}
      width={980}
      destroyOnClose
      footer={[
        <Button key="refresh" icon={<ReloadOutlined />} onClick={() => refreshStatus(true)} loading={loading}>
          刷新
        </Button>,
        <Button key="close" type="primary" onClick={onClose}>
          关闭
        </Button>,
      ]}
    >
      <Space direction="vertical" size={12} style={{ width: '100%' }}>
        <Text type="secondary">除 MySQL / Redis / Oracle / PostgreSQL 外，其他数据源需先安装启用后再连接。</Text>

        <Table
          rowKey="type"
          loading={loading}
          columns={columns as any}
          dataSource={rows}
          pagination={false}
          size="middle"
        />
      </Space>
    </Modal>
  );
};

export default DriverManagerModal;
