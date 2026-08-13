import type { ParserEngineInfo } from '@/api/system'

export const MINERU_IMAGE_EXTENSIONS = ['jpg', 'jpeg', 'png', 'bmp', 'tiff'] as const
export const PPTX_EXTENSIONS = ['pptx'] as const
export const LEGACY_PPT_EXTENSIONS = ['ppt'] as const

export function resolveDefaultParserEngineName(
  engines: ParserEngineInfo[],
  extensions: string[],
  useDeploymentDefaults = false,
): string {
  const supported = engines.filter(engine =>
    extensions.some(ext => (engine.FileTypes || []).includes(ext)),
  )
  if (useDeploymentDefaults) {
    const configured = supported.find(engine =>
      extensions.some(ext => (engine.DefaultFileTypes || []).includes(ext)),
    )
    if (configured) return configured.Name
  }
  return supported.find(engine => engine.Available !== false)?.Name ?? ''
}

export function resolveDeploymentRuleSeed<T>(
  current: T[],
  complete: T[],
  useDeploymentDefaults: boolean,
): T[] | null {
  if (!useDeploymentDefaults || complete.length === 0 || complete.length <= current.length) {
    return null
  }
  return complete
}
