package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	chatpipeline "github.com/Tencent/WeKnora/internal/application/service/chat_pipeline"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// Installed at the graph repository boundary so every chat search uses the same
// publication filter. The digest describes the connection created at startup.
type ProcessingGraphRepository struct {
	interfaces.RetrieveGraphRepository
	repo        *repository.ProcessingRepository
	destination string
}

func NewProcessingGraphRepository(inner interfaces.RetrieveGraphRepository, repo *repository.ProcessingRepository, enabled bool) *ProcessingGraphRepository {
	digest := ""
	if enabled && inner != nil {
		data, _ := json.Marshal([]string{os.Getenv("NEO4J_URI"), os.Getenv("NEO4J_USERNAME")})
		digest = fmt.Sprintf("%x", sha256.Sum256(data))
	}
	return &ProcessingGraphRepository{RetrieveGraphRepository: inner, repo: repo, destination: digest}
}

func (r *ProcessingGraphRepository) SearchNode(ctx context.Context, ns types.NameSpace, nodes []string) (*types.GraphData, error) {
	if r.destination == "" {
		return &types.GraphData{}, nil
	}
	var err error
	ns.ConfirmedContributions, ns.VisibleLegacyKnowledge, err = r.repo.VisibleGraph(ctx, ns, r.destination)
	if err != nil {
		return nil, err
	}
	slices.Sort(ns.ConfirmedContributions)
	slices.Sort(ns.VisibleLegacyKnowledge)
	graph, err := r.RetrieveGraphRepository.SearchNode(ctx, ns, nodes)
	if err != nil {
		return nil, err
	}
	// A publication or manual edit may race external I/O. Do not return an
	// earlier graph snapshot after its visibility has been withdrawn.
	confirmed, legacy, err := r.repo.VisibleGraph(ctx, ns, r.destination)
	if err != nil {
		return nil, err
	}
	slices.Sort(confirmed)
	slices.Sort(legacy)
	if !slices.Equal(confirmed, ns.ConfirmedContributions) || !slices.Equal(legacy, ns.VisibleLegacyKnowledge) {
		return nil, repository.ErrProcessingConflict
	}
	if graph == nil {
		graph = &types.GraphData{}
	}
	return graph, nil
}

type processingGraphOutput struct {
	ChunkID         string           `json:"chunk_id"`
	ContentRevision int              `json:"content_revision"`
	Graph           *types.GraphData `json:"graph"`
}

func (e *processingDocumentExecution) graphChunks(ctx context.Context) ([]*types.Chunk, error) {
	chunks, err := e.indexChunks(ctx, "chunk")
	if err != nil {
		return nil, err
	}
	images, err := e.indexChunks(ctx, "images")
	return append(chunks, images...), err
}

func (e *processingDocumentExecution) graphBarrier(ctx context.Context) (types.ProcessingOutcome, error) {
	graph, ok := e.s.graphEngine.(*ProcessingGraphRepository)
	if !ok || graph.destination == "" {
		return types.ProcessingOutcome{Status: types.ProcessingBlocked, ErrorClass: "configuration", ErrorCode: "GRAPH_DESTINATION_UNAVAILABLE", Message: "Enable and configure the graph destination before retrying"}, nil
	}
	chunks, err := e.graphChunks(ctx)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	if !e.lease.Step.PlanSealed {
		var specs []types.ProcessingStepSpec
		for _, chunk := range chunks {
			input, _ := json.Marshal(processingGraphOutput{ChunkID: chunk.ID, ContentRevision: chunk.ContentRevision})
			fingerprint := fmt.Sprintf("%x", sha256.Sum256(input))
			specs = append(specs,
				types.ProcessingStepSpec{Stage: "graph_extract", UnitKey: chunk.ID, Phase: e.lease.Step.Phase, Input: input, InputFingerprint: fingerprint},
				types.ProcessingStepSpec{Stage: "graph_apply", UnitKey: chunk.ID, Phase: e.lease.Step.Phase, Input: input, InputFingerprint: fingerprint, DependsOn: []string{"graph_extract/" + chunk.ID}})
		}
		next := time.Now().UTC().Add(time.Second)
		return types.ProcessingOutcome{Status: types.ProcessingWaitingExternal, NextRunAt: &next, SealPlan: true, ChildSteps: specs}, nil
	}
	for _, chunk := range chunks {
		var write types.ProcessingGraphWrite
		if err := e.dependency(ctx, "graph_apply", chunk.ID, "graph_apply", &write); err != nil {
			return types.ProcessingOutcome{}, err
		}
		if write.ChunkID != chunk.ID || write.ContentRevision != chunk.ContentRevision || write.PublicationEpoch != e.lease.Job.PublicationEpoch {
			return types.ProcessingOutcome{}, errors.New("GRAPH_COVERAGE_INVALID")
		}
	}
	return e.success(ctx, "graph", map[string]int{"confirmed_chunks": len(chunks)})
}

func (e *processingDocumentExecution) graphInput(ctx context.Context) (*types.Chunk, error) {
	var input processingGraphOutput
	if json.Unmarshal(e.lease.Step.Input, &input) != nil || input.ChunkID == "" || input.ChunkID != e.lease.Step.UnitKey {
		return nil, errors.New("GRAPH_INPUT_INVALID")
	}
	chunks, err := e.graphChunks(ctx)
	if err != nil {
		return nil, err
	}
	for _, chunk := range chunks {
		if chunk.ID != input.ChunkID {
			continue
		}
		current, err := e.s.chunkRepo.GetChunkByID(ctx, e.lease.Job.TenantID, chunk.ID)
		if err != nil {
			return nil, err
		}
		if input.ContentRevision != chunk.ContentRevision || current.ContentRevision != chunk.ContentRevision || current.Content != chunk.Content {
			return nil, errors.New("GRAPH_SOURCE_CHANGED")
		}
		return chunk, nil
	}
	return nil, errors.New("GRAPH_SOURCE_MISSING")
}

func (e *processingDocumentExecution) extractGraph(ctx context.Context) (types.ProcessingOutcome, error) {
	chunk, err := e.graphInput(ctx)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	cfg := e.kb.ExtractConfig
	if !e.kb.IsGraphEnabled() || cfg == nil || !cfg.Enabled || e.kb.SummaryModelID == "" || e.s.config.ExtractManager == nil || e.s.config.ExtractManager.ExtractGraph == nil {
		return types.ProcessingOutcome{}, errors.New("GRAPH_CONFIGURATION_INVALID")
	}
	model, err := e.s.modelService.GetChatModel(ctx, e.kb.SummaryModelID)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	extractor := chatpipeline.NewExtractor(model, &types.PromptTemplateStructured{
		Description: types.AppendCustomPromptInstructions(e.s.config.ExtractManager.ExtractGraph.Description, cfg.CustomInstructions, "graph_extraction"), Tags: cfg.Tags,
		Examples: []types.GraphData{{Text: cfg.Text, Node: cfg.Nodes, Relation: cfg.Relations}}})
	graph, err := extractor.Extract(ctx, chunk.Content)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	if err := validateProcessingGraph(graph, chunk.ID); err != nil {
		return types.ProcessingOutcome{}, err
	}
	return e.success(ctx, "graph_extract", processingGraphOutput{ChunkID: chunk.ID, ContentRevision: chunk.ContentRevision, Graph: graph})
}

func validateProcessingGraph(graph *types.GraphData, chunkID string) error {
	if graph == nil || len(graph.Node) > 512 || len(graph.Relation) > 2048 {
		return errors.New("GRAPH_OUTPUT_INVALID")
	}
	seen := map[string]bool{}
	for _, node := range graph.Node {
		if node == nil || strings.TrimSpace(node.Name) == "" || len(node.Name) > 1024 || seen[node.Name] || len(node.Attributes) > 128 {
			return errors.New("GRAPH_OUTPUT_INVALID")
		}
		for _, attribute := range node.Attributes {
			if len(attribute) > 16<<10 {
				return errors.New("GRAPH_OUTPUT_INVALID")
			}
		}
		seen[node.Name] = true
		node.Chunks = []string{chunkID}
	}
	for _, relation := range graph.Relation {
		if relation == nil || !seen[relation.Node1] || !seen[relation.Node2] || strings.TrimSpace(relation.Type) == "" || len(relation.Type) > 256 {
			return errors.New("GRAPH_OUTPUT_INVALID")
		}
	}
	return nil
}

func (e *processingDocumentExecution) applyGraph(ctx context.Context) (types.ProcessingOutcome, error) {
	graph, ok := e.s.graphEngine.(*ProcessingGraphRepository)
	if !ok || graph.destination == "" {
		return types.ProcessingOutcome{}, errors.New("GRAPH_DESTINATION_UNAVAILABLE")
	}
	chunk, err := e.graphInput(ctx)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	var output processingGraphOutput
	if err := e.dependency(ctx, "graph_extract", chunk.ID, "graph_extract", &output); err != nil {
		return types.ProcessingOutcome{}, err
	}
	if output.ChunkID != chunk.ID || output.ContentRevision != chunk.ContentRevision {
		return types.ProcessingOutcome{}, errors.New("GRAPH_SOURCE_CHANGED")
	}
	if err := validateProcessingGraph(output.Graph, chunk.ID); err != nil {
		return types.ProcessingOutcome{}, err
	}
	write, err := e.repo.ReserveGraphWrite(ctx, e.lease.Job.TenantID, e.lease, chunk.ID, chunk.ContentRevision, graph.destination)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	namespace := types.NameSpace{KnowledgeBase: e.kb.ID, Knowledge: e.lease.Job.KnowledgeID, ProcessingContribution: write.ID}
	writeErr := graph.AddGraph(ctx, namespace, []*types.GraphData{output.Graph})
	if err := e.repo.ValidateLease(ctx, e.lease.Job.TenantID, e.lease); err != nil {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		_ = graph.DelGraph(cleanup, []types.NameSpace{namespace})
		return types.ProcessingOutcome{}, err
	}
	if writeErr != nil {
		return types.ProcessingOutcome{}, writeErr
	}
	outcome, err := e.success(ctx, "graph_apply", write)
	outcome.GraphWriteID = write.ID
	return outcome, err
}
