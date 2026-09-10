package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/Tencent/WeKnora/internal/agent"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
)

func (s *knowledgeService) readProcessingWikiContribution(ctx context.Context, repo *repository.ProcessingRepository, write types.ProcessingWikiWrite) (*types.ProcessingWikiMutation, error) {
	job, err := repo.GetJob(ctx, write.TenantID, write.JobID)
	if err != nil {
		return nil, err
	}
	files := s.fileSvc
	backend, path, scoped := types.ParseStorageBackendPath(write.ArtifactRef)
	if !scoped {
		path = write.ArtifactRef
	}
	if s.storageResolver != nil {
		tenant, err := s.tenantRepo.GetTenantByID(ctx, write.TenantID)
		if err != nil {
			return nil, err
		}
		files, _, err = s.storageResolver.ResolveFileService(ctx, tenant, backend, types.ParseProviderScheme(path), strings.TrimSpace(os.Getenv("LOCAL_STORAGE_BASE_DIR")))
		if err != nil {
			return nil, err
		}
	} else if backend != "" {
		return nil, errors.New("WIKI_ARTIFACT_STORAGE_UNAVAILABLE")
	}
	data, err := NewProcessingArtifacts(files, s.resourceCatalog).Read(ctx, *job, types.ProcessingStep{ID: write.StepID, InputFingerprint: write.InputFingerprint}, "wiki_page", write.ArtifactRef, write.ArtifactDigest)
	if err != nil {
		return nil, err
	}
	var output types.ProcessingWikiMutation
	if json.Unmarshal(data, &output) != nil || output.Page == nil || output.Page.ID != write.PageID || output.Page.Slug != write.Slug || output.PublicationEpoch != write.PublicationEpoch || output.RetireOnly {
		return nil, errors.New("WIKI_CONTRIBUTION_INVALID")
	}
	return &output, nil
}

func (s *knowledgeService) retireProcessingWikiPage(ctx context.Context, repo *repository.ProcessingRepository, lease types.ProcessingLease, tenant *types.Tenant) (types.ProcessingOutcome, error) {
	kb, history, err := repo.WikiRetirementInput(ctx, lease.Job.TenantID, lease)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	files := s.fileSvc
	if s.storageResolver != nil {
		backend := ""
		if kb.StorageBackendID != nil {
			backend = *kb.StorageBackendID
		}
		files, _, err = s.storageResolver.ResolveFileService(ctx, tenant, backend, kb.GetStorageProvider(), strings.TrimSpace(os.Getenv("LOCAL_STORAGE_BASE_DIR")))
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
	} else if kb.StorageBackendID != nil && *kb.StorageBackendID != "" {
		return types.ProcessingOutcome{}, errors.New("WIKI_ARTIFACT_STORAGE_UNAVAILABLE")
	}
	e := processingDocumentExecution{s: s, repo: repo, lease: lease, kb: kb, artifacts: NewProcessingArtifacts(files, s.resourceCatalog)}
	previous, err := repo.WikiContributions(ctx, lease.Job.TenantID, lease, history.Slug)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	var ids []string
	for _, write := range previous {
		ids = append(ids, write.ID)
	}
	page, err := s.wikiRepo.GetByID(ctx, history.PageID)
	if err != nil && !errors.Is(err, repository.ErrWikiPageNotFound) {
		return types.ProcessingOutcome{}, err
	}
	exists := page != nil
	if !exists {
		page = &types.WikiPage{ID: history.PageID, TenantID: lease.Job.TenantID, KnowledgeBaseID: kb.ID, Slug: history.Slug, PageType: types.WikiPageTypeEntity, Status: types.WikiPageStatusArchived}
	}
	var mutation types.ProcessingWikiMutation
	out, cached, err := e.wikiCheckpoint(ctx, &mutation)
	if err != nil {
		return out, err
	}
	if cached && mutation.RetireOnly && mutation.Page != nil && mutation.Page.ID == page.ID && mutation.ExpectedRevision == page.MutationRevision &&
		mutation.PublicationEpoch == lease.Job.PublicationEpoch && slices.Equal(mutation.RetireIDs, ids) {
		out.WikiPage = &mutation
		return out, nil
	}
	mutation = types.ProcessingWikiMutation{RetireOnly: true, Page: page, ExpectedRevision: page.MutationRevision, PublicationEpoch: lease.Job.PublicationEpoch, RetireIDs: ids, PreserveUntracked: page.HasUntrackedContent()}
	if len(ids) > 0 && exists {
		var deleted strings.Builder
		removeChunks := map[string]bool{}
		for _, write := range previous {
			contribution, err := s.readProcessingWikiContribution(ctx, repo, write)
			if err != nil {
				return types.ProcessingOutcome{}, err
			}
			fmt.Fprintf(&deleted, "<document><title>%s</title><content>%s</content></document>\n", contribution.ContributionTitle, contribution.ContributionContent)
			mutation.PreserveUntracked = mutation.PreserveUntracked || contribution.PreserveUntracked
			for id := range contribution.ChunkRevisions {
				removeChunks[id] = true
			}
		}
		hadSource := false
		page.SourceRefs = slices.DeleteFunc(page.SourceRefs, func(ref string) bool {
			id, _, _ := strings.Cut(ref, "|")
			if id == lease.Job.KnowledgeID {
				hadSource = true
				return true
			}
			return false
		})
		page.ChunkRefs = slices.DeleteFunc(page.ChunkRefs, func(id string) bool { return removeChunks[id] })
		if hadSource {
			if len(page.SourceRefs) == 0 && !mutation.PreserveUntracked && types.NormalizeWikiEditSource(page.LastEditSource) == types.WikiEditSourcePipeline {
				page.Status, page.Content, page.Summary = types.WikiPageStatusArchived, "", ""
			} else if len(page.SourceRefs) > 0 || mutation.PreserveUntracked {
				model, err := s.modelService.GetChatModel(ctx, kb.SummaryModelID)
				if err != nil {
					return types.ProcessingOutcome{}, err
				}
				prompt := agent.WikiPageModifyUserPrompt
				inputs := map[string]string{
					"HasRetractions": "1", "PageSlug": page.Slug, "PageTitle": page.Title, "PageType": page.PageType, "PageAliases": strings.Join(page.Aliases, ", "),
					"ExistingContent": page.Content, "DeletedContent": deleted.String(), "RemainingSourcesContent": "Preserve all other sources and manual edits in the existing page.",
					"AvailableSlugs": strings.Join(page.OutLinks, "\n"), "Language": types.ResolveLanguageName(ctx, ""), "InstructionScope": "wiki_content"}
				if page.PageType == types.WikiPageTypeIndex {
					prompt = agent.WikiIndexIntroUpdatePrompt
					inputs["ExistingIntro"], inputs["ChangeDescription"] = page.Content, "Remove only these retired contributions; preserve all remaining sources and manual edits:\n"+deleted.String()
				}
				raw, err := e.wikiIngest().generateWithTemplate(ctx, model, prompt, inputs)
				if err != nil {
					return types.ProcessingOutcome{}, err
				}
				if strings.TrimSpace(raw) == "" || len(raw) > 1<<20 {
					return types.ProcessingOutcome{}, errors.New("WIKI_PAGE_OUTPUT_INVALID")
				}
				line, body := splitSummaryLine(raw)
				if body == "" {
					body = raw
				}
				page.Content = body
				if line != "" {
					page.Summary = line
				}
				if page.PageType == types.WikiPageTypeIndex {
					page.Content, page.Summary = strings.TrimSpace(raw), strings.TrimSpace(raw)
				}
			}
		}
		if mutation.PreserveUntracked {
			page.PreserveUntrackedContent()
		}
		stripWikiPageInlineChunkCitations(page)
		page.OutLinks = (&wikiPageService{}).parseOutLinks(page.Content)
	}
	out, err = e.wikiSuccess(ctx, mutation)
	out.WikiPage = &mutation
	return out, err
}
