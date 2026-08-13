import assert from 'node:assert/strict'
import test from 'node:test'

import {
  LEGACY_PPT_EXTENSIONS,
  MINERU_IMAGE_EXTENSIONS,
  PPTX_EXTENSIONS,
  resolveDefaultParserEngineName,
  resolveDeploymentRuleSeed,
} from './parserEngineDefaults'

const engines = [
  {
    Name: 'builtin',
    Description: 'builtin',
    FileTypes: ['docx', 'pdf', 'xls', 'xlsx', 'webp'],
    DefaultFileTypes: ['xls', 'xlsx'],
    Available: true,
  },
  {
    Name: 'mineru',
    Description: 'mineru',
    FileTypes: ['pdf', 'ppt', 'pptx'],
    DefaultFileTypes: ['pdf', 'pptx'],
    Available: true,
  },
  {
    Name: 'markitdown',
    Description: 'markitdown',
    FileTypes: ['ppt', 'pptx'],
    DefaultFileTypes: ['ppt'],
    Available: true,
  },
]

test('deployment parser defaults select MinerU for PDF, MarkItDown for legacy PPT, and builtin for Excel', () => {
  assert.equal(resolveDefaultParserEngineName(engines, ['pdf'], true), 'mineru')
  assert.equal(resolveDefaultParserEngineName(engines, ['ppt'], true), 'markitdown')
  assert.equal(resolveDefaultParserEngineName(engines, ['pptx'], true), 'mineru')
  assert.equal(resolveDefaultParserEngineName(engines, ['xls', 'xlsx'], true), 'builtin')
})

test('unconfigured file types retain the first available engine', () => {
  assert.equal(resolveDefaultParserEngineName(engines, ['docx']), 'builtin')
  assert.equal(resolveDefaultParserEngineName(engines, ['webp']), 'builtin')
})

test('a configured deployment default remains selected when temporarily unavailable', () => {
  const unavailableMinerU = engines.map(engine =>
    engine.Name === 'mineru' ? { ...engine, Available: false } : engine,
  )
  assert.equal(resolveDefaultParserEngineName(unavailableMinerU, ['pdf'], true), 'mineru')
  assert.equal(resolveDefaultParserEngineName(unavailableMinerU, ['pdf'], false), 'builtin')

  const unavailableBuiltin = [
    ...engines.map(engine => engine.Name === 'builtin' ? { ...engine, Available: false } : engine),
    {
      Name: 'weknoracloud',
      Description: 'cloud',
      FileTypes: ['xls', 'xlsx'],
      DefaultFileTypes: [],
      Available: true,
    },
  ]
  assert.equal(resolveDefaultParserEngineName(unavailableBuiltin, ['xls', 'xlsx'], true), 'builtin')
})

test('deployment rules seed only a new knowledge base and preserve explicit empty shared contexts', () => {
  const complete = [{ file_types: ['pdf'], engine: 'mineru' }]
  assert.deepEqual(resolveDeploymentRuleSeed([], complete, true), complete)
  assert.equal(resolveDeploymentRuleSeed([], complete, false), null)
  assert.equal(resolveDeploymentRuleSeed(complete, complete, true), null)
})

test('component extension groups do not mix unsupported image or legacy PowerPoint formats', () => {
  assert.deepEqual([...PPTX_EXTENSIONS], ['pptx'])
  assert.deepEqual([...LEGACY_PPT_EXTENSIONS], ['ppt'])
  assert.deepEqual([...MINERU_IMAGE_EXTENSIONS], ['jpg', 'jpeg', 'png', 'bmp', 'tiff'])
  assert.equal(MINERU_IMAGE_EXTENSIONS.includes('webp' as never), false)
  assert.equal(MINERU_IMAGE_EXTENSIONS.includes('gif' as never), false)
})
