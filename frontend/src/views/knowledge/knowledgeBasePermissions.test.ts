import assert from 'node:assert/strict'
import test from 'node:test'

import { canAddKnowledgeToBase, canEditKnowledgeBaseContent } from './knowledgeBasePermissions'

test('an invited workspace Contributor can add content to an existing KB', () => {
  assert.equal(canAddKnowledgeToBase({
    isViaShare: false,
    isCreator: false,
    isTenantAdmin: false,
    isTenantContributor: true,
    sharedPermissionCanEdit: false,
  }), true)
})

test('a workspace Viewer cannot add content to an existing KB', () => {
  assert.equal(canAddKnowledgeToBase({
    isViaShare: false,
    isCreator: false,
    isTenantAdmin: false,
    isTenantContributor: false,
    sharedPermissionCanEdit: false,
  }), false)
})

test('an invited workspace Contributor can manage existing KB documents', () => {
  assert.equal(canEditKnowledgeBaseContent({
    isViaShare: false,
    isCreator: false,
    isTenantAdmin: false,
    isTenantContributor: true,
    sharedPermissionCanEdit: false,
  }), true)
})

test('an organization share uses the explicit share permission', () => {
  assert.equal(canAddKnowledgeToBase({
    isViaShare: true,
    isCreator: true,
    isTenantAdmin: true,
    isTenantContributor: true,
    sharedPermissionCanEdit: false,
  }), false)
  assert.equal(canAddKnowledgeToBase({
    isViaShare: true,
    isCreator: false,
    isTenantAdmin: false,
    isTenantContributor: false,
    sharedPermissionCanEdit: true,
  }), true)
})
