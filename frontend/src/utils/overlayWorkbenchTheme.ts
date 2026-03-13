type OverlayWorkbenchTheme = {
  isDark: boolean;
  shellBg: string;
  shellBorder: string;
  shellShadow: string;
  shellBackdropFilter: string;
  sectionBg: string;
  sectionBorder: string;
  mutedText: string;
  titleText: string;
  iconBg: string;
  iconColor: string;
  hoverBg: string;
  selectedBg: string;
  selectedText: string;
  divider: string;
};

export const buildOverlayWorkbenchTheme = (darkMode: boolean): OverlayWorkbenchTheme => {
  if (darkMode) {
    return {
      isDark: true,
      shellBg: 'linear-gradient(180deg, rgba(15, 15, 17, 0.96) 0%, rgba(11, 11, 13, 0.98) 100%)',
      shellBorder: '1px solid rgba(255,255,255,0.08)',
      shellShadow: '0 24px 56px rgba(0,0,0,0.34)',
      shellBackdropFilter: 'blur(18px)',
      sectionBg: 'rgba(255,255,255,0.03)',
      sectionBorder: '1px solid rgba(255,255,255,0.08)',
      mutedText: 'rgba(255,255,255,0.5)',
      titleText: '#f5f7ff',
      iconBg: 'rgba(255,214,102,0.12)',
      iconColor: '#ffd666',
      hoverBg: 'rgba(255,214,102,0.10)',
      selectedBg: 'rgba(255,214,102,0.14)',
      selectedText: '#ffd666',
      divider: 'rgba(255,255,255,0.08)',
    };
  }

  return {
    isDark: false,
    shellBg: 'linear-gradient(180deg, rgba(255,255,255,0.98) 0%, rgba(246,248,252,0.98) 100%)',
    shellBorder: '1px solid rgba(16,24,40,0.08)',
    shellShadow: '0 18px 42px rgba(15,23,42,0.12)',
    shellBackdropFilter: 'none',
    sectionBg: 'rgba(255,255,255,0.84)',
    sectionBorder: '1px solid rgba(16,24,40,0.08)',
    mutedText: 'rgba(16,24,40,0.55)',
    titleText: '#162033',
    iconBg: 'rgba(24,144,255,0.1)',
    iconColor: '#1677ff',
    hoverBg: 'rgba(24,144,255,0.08)',
    selectedBg: 'rgba(24,144,255,0.12)',
    selectedText: '#1677ff',
    divider: 'rgba(16,24,40,0.08)',
  };
};

export type { OverlayWorkbenchTheme };
