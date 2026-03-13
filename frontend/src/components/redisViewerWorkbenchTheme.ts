type RedisWorkbenchThemeInput = {
  darkMode: boolean;
  opacity: number;
  blur: number;
};

type RedisWorkbenchTheme = {
  isDark: boolean;
  appBg: string;
  panelBg: string;
  panelBgStrong: string;
  panelBgSubtle: string;
  panelBorder: string;
  panelInset: string;
  toolbarPrimaryBg: string;
  contentEmptyBg: string;
  textPrimary: string;
  textSecondary: string;
  textMuted: string;
  accent: string;
  accentSoft: string;
  accentBorder: string;
  actionSecondaryBg: string;
  actionSecondaryBorder: string;
  actionDangerBg: string;
  actionDangerBorder: string;
  actionDangerText: string;
  statusTagBg: string;
  statusTagBorder: string;
  statusTagMutedBg: string;
  statusTagMutedBorder: string;
  treeHoverBg: string;
  treeSelectedBg: string;
  treeSelectedBorder: string;
  divider: string;
  shadow: string;
  backdropFilter: string;
};

const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value));

export const buildRedisWorkbenchTheme = ({
  darkMode,
  opacity,
  blur,
}: RedisWorkbenchThemeInput): RedisWorkbenchTheme => {
  const normalizedOpacity = clamp(opacity, 0.1, 1);
  const normalizedBlur = Math.max(0, Math.round(blur));
  const isTranslucent = normalizedOpacity < 0.999 || normalizedBlur > 0;

  if (darkMode) {
    const appTopAlpha = isTranslucent ? Math.max(0.08, Math.min(0.22, normalizedOpacity * 0.16)) : 0.92;
    const appBottomAlpha = isTranslucent ? Math.max(0.12, Math.min(0.28, normalizedOpacity * 0.22)) : 0.96;
    const panelAlpha = isTranslucent ? Math.max(0.06, Math.min(0.16, normalizedOpacity * 0.1)) : 0.34;
    const strongAlpha = isTranslucent ? Math.max(0.1, Math.min(0.22, normalizedOpacity * 0.16)) : 0.42;
    const subtleAlpha = isTranslucent ? Math.max(0.03, Math.min(0.08, normalizedOpacity * 0.05)) : 0.08;
    return {
      isDark: true,
      appBg: `linear-gradient(180deg, rgba(15, 15, 17, ${appTopAlpha}) 0%, rgba(11, 11, 13, ${appBottomAlpha}) 100%)`,
      panelBg: `rgba(24, 24, 28, ${panelAlpha})`,
      panelBgStrong: `rgba(31, 31, 36, ${strongAlpha})`,
      panelBgSubtle: `rgba(255, 255, 255, ${subtleAlpha})`,
      panelBorder: `1px solid rgba(255, 255, 255, ${isTranslucent ? Math.max(0.12, Math.min(0.24, normalizedOpacity * 0.2)) : 0.08})`,
      panelInset: `inset 0 1px 0 rgba(255,255,255,${isTranslucent ? Math.max(0.05, Math.min(0.12, normalizedOpacity * 0.1)) : 0.04})`,
      toolbarPrimaryBg: `linear-gradient(135deg, rgba(246,196,83,0.22) 0%, rgba(246,196,83,0.12) 100%)`,
      contentEmptyBg: `linear-gradient(180deg, rgba(255,255,255,0.03) 0%, rgba(255,255,255,0.015) 100%)`,
      textPrimary: 'rgba(245, 247, 251, 0.96)',
      textSecondary: 'rgba(218, 224, 235, 0.82)',
      textMuted: 'rgba(168, 177, 194, 0.72)',
      accent: '#f6c453',
      accentSoft: 'rgba(246, 196, 83, 0.18)',
      accentBorder: 'rgba(246, 196, 83, 0.3)',
      actionSecondaryBg: 'rgba(255, 255, 255, 0.04)',
      actionSecondaryBorder: 'rgba(255, 255, 255, 0.09)',
      actionDangerBg: 'rgba(255, 95, 95, 0.12)',
      actionDangerBorder: 'rgba(255, 95, 95, 0.28)',
      actionDangerText: '#ff8f8f',
      statusTagBg: 'rgba(25, 106, 255, 0.16)',
      statusTagBorder: 'rgba(25, 106, 255, 0.28)',
      statusTagMutedBg: 'rgba(255, 255, 255, 0.04)',
      statusTagMutedBorder: 'rgba(255, 255, 255, 0.08)',
      treeHoverBg: 'rgba(255, 255, 255, 0.045)',
      treeSelectedBg: 'linear-gradient(90deg, rgba(246,196,83,0.2) 0%, rgba(246,196,83,0.08) 100%)',
      treeSelectedBorder: 'rgba(246, 196, 83, 0.24)',
      divider: 'rgba(255, 255, 255, 0.07)',
      shadow: '0 20px 48px rgba(0, 0, 0, 0.26)',
      backdropFilter: normalizedBlur > 0 ? `blur(${normalizedBlur}px)` : 'none',
    };
  }

  const appTopAlpha = isTranslucent ? Math.max(0.16, Math.min(0.36, normalizedOpacity * 0.24)) : 0.98;
  const appBottomAlpha = isTranslucent ? Math.max(0.22, Math.min(0.44, normalizedOpacity * 0.32)) : 0.96;
  const panelAlpha = isTranslucent ? Math.max(0.18, Math.min(0.4, normalizedOpacity * 0.26)) : 0.94;
  const strongAlpha = isTranslucent ? Math.max(0.26, Math.min(0.52, normalizedOpacity * 0.34)) : 0.98;
  return {
    isDark: false,
    appBg: `linear-gradient(180deg, rgba(248, 250, 252, ${appTopAlpha}) 0%, rgba(242, 245, 248, ${appBottomAlpha}) 100%)`,
    panelBg: `rgba(255, 255, 255, ${panelAlpha})`,
    panelBgStrong: `rgba(255, 255, 255, ${strongAlpha})`,
    panelBgSubtle: 'rgba(15, 23, 42, 0.03)',
    panelBorder: `1px solid rgba(15, 23, 42, ${isTranslucent ? Math.max(0.1, Math.min(0.18, normalizedOpacity * 0.12)) : 0.08})`,
    panelInset: `inset 0 1px 0 rgba(255,255,255,${isTranslucent ? 0.38 : 0.72})`,
    toolbarPrimaryBg: 'linear-gradient(135deg, rgba(22,119,255,0.12) 0%, rgba(22,119,255,0.06) 100%)',
    contentEmptyBg: 'linear-gradient(180deg, rgba(15,23,42,0.02) 0%, rgba(15,23,42,0.01) 100%)',
    textPrimary: 'rgba(15, 23, 42, 0.92)',
    textSecondary: 'rgba(51, 65, 85, 0.82)',
    textMuted: 'rgba(100, 116, 139, 0.76)',
    accent: '#1677ff',
    accentSoft: 'rgba(22, 119, 255, 0.12)',
    accentBorder: 'rgba(22, 119, 255, 0.22)',
    actionSecondaryBg: 'rgba(255, 255, 255, 0.72)',
    actionSecondaryBorder: 'rgba(15, 23, 42, 0.08)',
    actionDangerBg: 'rgba(255, 77, 79, 0.08)',
    actionDangerBorder: 'rgba(255, 77, 79, 0.24)',
    actionDangerText: '#cf1322',
    statusTagBg: 'rgba(22, 119, 255, 0.1)',
    statusTagBorder: 'rgba(22, 119, 255, 0.16)',
    statusTagMutedBg: 'rgba(15, 23, 42, 0.04)',
    statusTagMutedBorder: 'rgba(15, 23, 42, 0.08)',
    treeHoverBg: 'rgba(15, 23, 42, 0.035)',
    treeSelectedBg: 'linear-gradient(90deg, rgba(22,119,255,0.12) 0%, rgba(22,119,255,0.05) 100%)',
    treeSelectedBorder: 'rgba(22, 119, 255, 0.18)',
    divider: 'rgba(15, 23, 42, 0.08)',
    shadow: '0 22px 52px rgba(15, 23, 42, 0.08)',
    backdropFilter: normalizedBlur > 0 ? `blur(${normalizedBlur}px)` : 'none',
  };
};

export type { RedisWorkbenchTheme, RedisWorkbenchThemeInput };
