export interface AddKnowledgePermissionInput {
  isViaShare: boolean
  isCreator: boolean
  isTenantAdmin: boolean
  isTenantContributor: boolean
  sharedPermissionCanEdit: boolean
}

/**
 * Adding a document is a content-editor operation, not a knowledge-base
 * ownership operation. In the KB's home workspace a Contributor may add new
 * content; when reached through an organization share, the explicit share
 * permission remains authoritative.
 */
export function canAddKnowledgeToBase(input: AddKnowledgePermissionInput): boolean {
  if (input.isViaShare) return input.sharedPermissionCanEdit
  return input.isCreator
    || input.isTenantAdmin
    || input.isTenantContributor
    || input.sharedPermissionCanEdit
}
