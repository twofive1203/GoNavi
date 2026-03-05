# 贡献指南

感谢你对本项目的贡献。

本项目采用“发布优先（`main` 为默认分支）+ `release/*` 分支发版”的协作模型。为减少分支漂移与 PR 处理成本，请在提交贡献前先阅读本指南。

---

## 分支模型

- `main`：稳定发布分支，也是仓库默认分支
- `dev`：日常开发集成分支，主要供维护者使用
- `release/*`：发布准备分支，主要供维护者使用
- 外部贡献者建议使用以下分支命名：
  - `fix/*`：问题修复
  - `feature/*`：功能新增或增强

维护者发布流转如下：

```text
feature/* / fix/* -> dev -> release/* -> main -> tag(vX.Y.Z)
```

---

## 外部贡献者如何提 Pull Request

无论是 `fix/*` 还是 `feature/*`，**外部贡献者统一直接向 `main` 发起 Pull Request**。

这样做的原因：

- `main` 是默认分支，PR 入口更直观
- 合并后贡献会直接体现在默认分支
- 便于维护者统一做后续同步与发版整理

建议流程：

1. Fork 本仓库
2. 从你自己的仓库创建分支（建议命名为 `fix/*` 或 `feature/*`）
3. 完成代码修改，并进行必要自检
4. 推送到你的远程分支
5. 向本仓库的 `main` 分支发起 Pull Request

---

## Pull Request 要求

请尽量保证 PR 单一、清晰、可审核。

建议遵循以下要求：

- 一个 PR 只解决一类问题，避免混入无关改动
- 标题清晰说明改动目的
- 描述中说明：
  - 背景与问题
  - 变更点
  - 影响范围
  - 验证方式
- 如涉及 UI 调整，建议附截图或录屏
- 如涉及兼容性、数据变更或构建链路调整，请明确说明风险和回滚方式

---

## PR 合并策略（维护者）

`main` 分支上的 PR 建议使用 **Squash and merge**。

原因：

- 保持 `main` 历史干净、线性
- 每个 PR 在 `main` 上对应一个清晰提交
- 降低发布排查与回滚成本

---

## 维护者同步规则

由于外部 PR 会直接合入 `main`，维护者必须及时将 `main` 的变更同步到开发与发布分支，避免分支漂移。

### 1. main → dev 同步（必做）

任何合入 `main` 的变更，都必须同步到 `dev`：

```bash
git checkout dev
git pull
git merge main
git push
```

### 2. 发版前从 dev 切 release/*

发布前由维护者基于 `dev` 创建发布分支，例如：

```bash
git checkout dev
git pull
git checkout -b release/v0.6.0
git push -u origin release/v0.6.0
```

### 3. release/* → main 发版

发布准备完成后，将 `release/*` 合并回 `main`，并打标签发布：

```bash
git checkout main
git pull
git merge release/v0.6.0
git push
git tag v0.6.0
git push origin v0.6.0
```

### 4. main 回流到 dev（发版后必做）

发布完成后，再次将 `main` 回流到 `dev`，确保开发线与发布线一致：

```bash
git checkout dev
git pull
git merge main
git push
```

---

## 提交建议

建议保持提交信息简洁、明确，便于维护者审查与后续追踪。

推荐格式：

```text
emoji type(scope): 中文描述
```

示例：

```text
🔧 fix(ci): 修复 Windows AMD64 下 DuckDB 驱动构建工具链
✨ feat(redis): 新增 Stream 类型数据浏览支持
♻️ refactor(datagrid): 优化大表横向滚动与渲染结构
```

---

## 其他说明

- 文档、构建链路、驱动兼容性相关改动，请尽量附带验证结果
- 若改动较大，建议先提 Issue 或 Draft PR，先对齐方案再实施
- 如提交内容与项目当前架构方向冲突，维护者可能要求收敛范围后再合并

感谢你的贡献。
