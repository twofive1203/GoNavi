#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

DEFAULT_DRIVERS=(mariadb doris sphinx sqlserver sqlite duckdb dameng kingbase highgo vastbase mongodb tdengine clickhouse)

usage() {
  cat <<'EOF'
用法：
  ./build-driver-agents.sh [选项]

选项：
  --drivers <列表>      指定驱动列表（逗号分隔），例如：kingbase,mongodb
  --platform <GOOS/GOARCH>
                        目标平台，默认使用当前 Go 环境（go env GOOS/GOARCH）
  --out-dir <目录>      输出目录根路径，默认：dist/driver-agents
  --bundle-name <文件名> 驱动总包 zip 名称，默认：GoNavi-DriverAgents.zip
  --strict              任一驱动构建失败即中断（默认失败后继续，最后汇总）
  -h, --help            显示帮助

示例：
  ./build-driver-agents.sh
  ./build-driver-agents.sh --drivers kingbase
  ./build-driver-agents.sh --platform windows/amd64 --drivers kingbase,mongodb
EOF
}

normalize_driver() {
  local name
  name="$(echo "${1:-}" | tr '[:upper:]' '[:lower:]' | xargs)"
  case "$name" in
    doris|diros) echo "doris" ;;
    mariadb|sphinx|sqlserver|sqlite|duckdb|dameng|kingbase|highgo|vastbase|mongodb|tdengine|clickhouse)
      echo "$name"
      ;;
    *)
      return 1
      ;;
  esac
}

build_driver_name() {
  case "$1" in
    doris) echo "diros" ;;
    *) echo "$1" ;;
  esac
}

platform_dir_name() {
  case "$1" in
    windows) echo "Windows" ;;
    darwin) echo "MacOS" ;;
    linux) echo "Linux" ;;
    *) echo "Unknown" ;;
  esac
}

driver_csv=""
target_platform=""
out_root="dist/driver-agents"
bundle_name="GoNavi-DriverAgents.zip"
strict_mode="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --drivers)
      driver_csv="${2:-}"
      shift 2
      ;;
    --platform)
      target_platform="${2:-}"
      shift 2
      ;;
    --out-dir)
      out_root="${2:-}"
      shift 2
      ;;
    --bundle-name)
      bundle_name="${2:-}"
      shift 2
      ;;
    --strict)
      strict_mode="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "❌ 未知参数：$1"
      usage
      exit 1
      ;;
  esac
done

if ! command -v go >/dev/null 2>&1; then
  echo "❌ 未找到 Go，请先安装 Go 并确保 go 在 PATH 中。"
  exit 1
fi

if [[ -z "$target_platform" ]]; then
  target_platform="$(go env GOOS)/$(go env GOARCH)"
fi

if [[ "$target_platform" != */* ]]; then
  echo "❌ --platform 参数格式错误，应为 GOOS/GOARCH，例如 darwin/arm64"
  exit 1
fi

goos="${target_platform%%/*}"
goarch="${target_platform##*/}"
platform_key="${goos}-${goarch}"
platform_dir="$(platform_dir_name "$goos")"

declare -a drivers=()
if [[ -n "$driver_csv" ]]; then
  IFS=',' read -r -a raw_drivers <<<"$driver_csv"
  for item in "${raw_drivers[@]}"; do
    normalized="$(normalize_driver "$item")" || {
      echo "❌ 不支持的驱动：$item"
      exit 1
    }
    drivers+=("$normalized")
  done
else
  drivers=("${DEFAULT_DRIVERS[@]}")
fi

output_dir="${out_root%/}/${platform_key}"
bundle_stage_dir="$(mktemp -d "${TMPDIR:-/tmp}/gonavi-driver-bundle.XXXXXX")"
bundle_platform_dir="$bundle_stage_dir/$platform_dir"

cleanup() {
  rm -rf "$bundle_stage_dir"
}
trap cleanup EXIT

mkdir -p "$output_dir" "$bundle_platform_dir"
output_dir_abs="$(cd "$output_dir" && pwd)"
bundle_zip_path="$output_dir_abs/$bundle_name"

declare -a built_assets=()
declare -a failed_drivers=()
declare -a skipped_drivers=()

echo "🚀 开始构建 optional-driver-agent"
echo "   平台：$goos/$goarch"
echo "   输出目录：$output_dir_abs"
echo "   驱动列表：${drivers[*]}"

for driver in "${drivers[@]}"; do
  if [[ "$driver" == "duckdb" && "$goos" == "windows" && "$goarch" != "amd64" ]]; then
    echo "⚠️  跳过 duckdb（仅支持 windows/amd64）"
    skipped_drivers+=("$driver")
    continue
  fi

  build_driver="$(build_driver_name "$driver")"
  tag="gonavi_${build_driver}_driver"
  asset_name="${driver}-driver-agent-${goos}-${goarch}"
  if [[ "$goos" == "windows" ]]; then
    asset_name="${asset_name}.exe"
  fi
  output_path="$output_dir_abs/$asset_name"

  cgo_enabled=0
  if [[ "$driver" == "duckdb" ]]; then
    cgo_enabled=1
  fi

  echo "🔧 构建 $driver -> $asset_name (tag=$tag, CGO_ENABLED=$cgo_enabled)"
  set +e
  CGO_ENABLED="$cgo_enabled" GOOS="$goos" GOARCH="$goarch" GOTOOLCHAIN=auto \
    go build -tags "$tag" -trimpath -ldflags "-s -w" -o "$output_path" ./cmd/optional-driver-agent
  build_exit=$?
  set -e

  if [[ $build_exit -ne 0 ]]; then
    echo "❌ 构建失败：$driver"
    failed_drivers+=("$driver")
    if [[ "$strict_mode" == "true" ]]; then
      exit $build_exit
    fi
    continue
  fi

  cp "$output_path" "$bundle_platform_dir/$asset_name"
  built_assets+=("$asset_name")
done

if [[ ${#built_assets[@]} -eq 0 ]]; then
  echo "❌ 未成功构建任何驱动代理。"
  exit 1
fi

rm -f "$bundle_zip_path"
if command -v zip >/dev/null 2>&1; then
  (
    cd "$bundle_stage_dir"
    zip -qry "$bundle_zip_path" "$platform_dir"
  )
elif command -v ditto >/dev/null 2>&1; then
  (
    cd "$bundle_stage_dir"
    ditto -c -k --sequesterRsrc --keepParent "$platform_dir" "$bundle_zip_path"
  )
else
  echo "❌ 未找到 zip/ditto，无法生成驱动总包 zip。"
  exit 1
fi

echo ""
echo "✅ 构建完成"
echo "   单文件输出目录：$output_dir_abs"
echo "   驱动总包：$bundle_zip_path"
echo "   已构建：${built_assets[*]}"
if [[ ${#skipped_drivers[@]} -gt 0 ]]; then
  echo "   已跳过：${skipped_drivers[*]}"
fi
if [[ ${#failed_drivers[@]} -gt 0 ]]; then
  echo "⚠️  构建失败驱动：${failed_drivers[*]}"
  exit 2
fi
