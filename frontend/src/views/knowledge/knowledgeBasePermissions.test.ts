import assert from 'node:assert/strict'
import test from 'node:test'

import {
  canAccessOriginalKnowledgeFile,
  canAddKnowledgeToBase,
  canEditKnowledgeBaseContent,
} from './knowledgeBasePermissions'

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

test('only the source workspace Owner can access original file bytes', () => {
  assert.equal(canAccessOriginalKnowledgeFile({
    isWorkspaceOwner: true,
    isViaShare: false,
    activeTenantId: 10006,
    knowledgeTenantId: 10006,
  }), true)
  assert.equal(canAccessOriginalKnowledgeFile({
    isWorkspaceOwner: false,
    isViaShare: false,
    activeTenantId: 10006,
    knowledgeTenantId: 10006,
  }), false)
  assert.equal(canAccessOriginalKnowledgeFile({
    isWorkspaceOwner: true,
    isViaShare: true,
    activeTenantId: 10003,
    knowledgeTenantId: 10006,
  }), false)
})
