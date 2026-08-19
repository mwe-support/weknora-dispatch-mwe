import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'

const source = readFileSync(new URL('./TenantInfo.vue', import.meta.url), 'utf8')

test('renaming the active workspace refreshes the selected tenant name cache', () => {
  const saveStart = source.indexOf('const saveTenantName = async () =>')
  const methodsStart = source.indexOf('// Methods', saveStart)
  assert.notEqual(saveStart, -1)
  assert.notEqual(methodsStart, -1)
  const saveTenantName = source.slice(saveStart, methodsStart)

  assert.match(
    saveTenantName,
    /authStore\.setSelectedTenant\(Number\(tenantInfo\.value\.id\), newName\)/,
    'rename success must replace selectedTenantName/localStorage without requiring a tenant switch',
  )
})
