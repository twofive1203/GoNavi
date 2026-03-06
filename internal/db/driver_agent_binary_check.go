package db

import (
	"debug/pe"
	"fmt"
	"runtime"
	"strings"
)

const (
	peMachineI386  uint16 = 0x014c
	peMachineAmd64 uint16 = 0x8664
	peMachineArm64 uint16 = 0xaa64
)

func windowsMachineLabel(machine uint16) string {
	switch machine {
	case peMachineI386:
		return "windows-386"
	case peMachineAmd64:
		return "windows-amd64"
	case peMachineArm64:
		return "windows-arm64"
	default:
		return fmt.Sprintf("windows-unknown(0x%04x)", machine)
	}
}

func expectedWindowsMachineForGoArch(goarch string) (uint16, string, bool) {
	switch strings.ToLower(strings.TrimSpace(goarch)) {
	case "386":
		return peMachineI386, "windows-386", true
	case "amd64":
		return peMachineAmd64, "windows-amd64", true
	case "arm64":
		return peMachineArm64, "windows-arm64", true
	default:
		return 0, "", false
	}
}

func validateWindowsExecutableMachine(pathText string) error {
	file, err := pe.Open(pathText)
	if err != nil {
		return fmt.Errorf("无法识别为有效的 Windows 可执行文件：%w", err)
	}
	defer file.Close()

	expectedMachine, expectedLabel, ok := expectedWindowsMachineForGoArch(runtime.GOARCH)
	if !ok {
		return nil
	}
	actualMachine := file.FileHeader.Machine
	if actualMachine != expectedMachine {
		return fmt.Errorf("可执行文件架构不兼容（文件=%s，当前进程=%s）", windowsMachineLabel(actualMachine), expectedLabel)
	}
	return nil
}

// ValidateOptionalDriverAgentExecutable 校验可选驱动代理二进制是否可在当前进程中执行。
// 当前主要用于 Windows 下的 PE 架构兼容性校验，避免升级后复用到错误架构的旧代理。
func ValidateOptionalDriverAgentExecutable(driverType string, executablePath string) error {
	pathText := strings.TrimSpace(executablePath)
	if pathText == "" {
		return fmt.Errorf("%s 驱动代理路径为空", driverDisplayName(driverType))
	}
	if runtime.GOOS != "windows" {
		return nil
	}
	if err := validateWindowsExecutableMachine(pathText); err != nil {
		return fmt.Errorf("%s 驱动代理不可用：%w", driverDisplayName(driverType), err)
	}
	return nil
}
