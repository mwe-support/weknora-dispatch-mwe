/**
 * `/platform/settings` renders Settings through the child RouterView. Every
 * other platform page keeps one global Settings host so menu actions can open
 * it as a modal without navigating away.
 */
export function shouldMountGlobalSettings(routePath: string): boolean {
  return routePath !== '/platform/settings'
}
