import assert from 'node:assert/strict'
import test from 'node:test'

import { shouldMountGlobalSettings } from './settingsHost'

test('the dedicated settings route owns the only settings instance', () => {
  assert.equal(shouldMountGlobalSettings('/platform/settings'), false)
})

test('other platform routes keep the global settings host available', () => {
  assert.equal(shouldMountGlobalSettings('/platform/knowledge-bases'), true)
  assert.equal(shouldMountGlobalSettings('/platform/knowledge-bases/kb-1'), true)
})
