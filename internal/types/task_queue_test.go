package types

import "testing"

func TestLocalNormalizationDoesNotWaitBehindSourceIO(t *testing.T) {
	queue := ProcessingQueue("normalize")
	if QueueWeightsForPool(WorkerPoolCore)[queue] == 0 || QueueWeightsForSharedPool()[queue] == 0 {
		t.Fatal("ready local normalization cannot run while the source pool is occupied")
	}
	if QueueWeightsForPool(WorkerPoolMaintenance)[queue] != 0 {
		t.Fatal("local normalization still shares source I/O capacity")
	}
}

func TestExportReceiptsDoNotWaitBehindSourceScanFIFO(t *testing.T) {
	for _, stage := range []string{"export_start", "export_poll", "download"} {
		if ProcessingQueue(stage) != QueueExport {
			t.Fatalf("%s shares the scan FIFO", stage)
		}
	}
	if ProcessingQueue("scan_page") != QueueSync || ProcessingQueue("native_read") != QueueSync {
		t.Fatal("scan routing changed")
	}
	weights := QueueWeightsForPool(WorkerPoolMaintenance)
	if weights[QueueExport] <= weights[QueueSync] {
		t.Fatal("expiring exports must have a dequeue preference")
	}
	if queue, ok := QueueForTaskType(TypeProcessingStep + ":" + QueueExport); !ok || queue != QueueExport {
		t.Fatal("export processing task has no consumer")
	}
	if QueueWeightsForSharedPool()[QueueExport] != 0 {
		t.Fatal("export calls escaped the maintenance capacity limit")
	}
}

func TestQueueDefinitionsAreUniqueAndConsumable(t *testing.T) {
	definitions := QueueDefinitions()
	if len(definitions) == 0 {
		t.Fatal("queue registry must not be empty")
	}

	validPools := map[string]bool{
		WorkerPoolCore:        true,
		WorkerPoolPostProcess: true,
		WorkerPoolEnrichment:  true,
		WorkerPoolMaintenance: true,
		WorkerPoolWiki:        true,
	}
	seen := make(map[string]bool, len(definitions))
	seenTaskTypes := make(map[string]string)
	for _, definition := range definitions {
		if seen[definition.Name] {
			t.Fatalf("duplicate queue definition %q", definition.Name)
		}
		seen[definition.Name] = true
		if !validPools[definition.Pool] {
			t.Fatalf("queue %q references unknown pool %q", definition.Name, definition.Pool)
		}
		if definition.Weight <= 0 {
			t.Fatalf("queue %q has non-positive weight %d", definition.Name, definition.Weight)
		}
		if len(definition.TaskTypes) == 0 {
			t.Fatalf("queue %q declares no task types", definition.Name)
		}
		for _, taskType := range definition.TaskTypes {
			if previousQueue, exists := seenTaskTypes[taskType]; exists {
				t.Fatalf("task type %q is declared by both %q and %q", taskType, previousQueue, definition.Name)
			}
			seenTaskTypes[taskType] = definition.Name
			queue, ok := QueueForTaskType(taskType)
			if !ok || queue != definition.Name {
				t.Fatalf("task type %q resolves to queue %q, want %q", taskType, queue, definition.Name)
			}
		}
	}

	for pool := range validPools {
		if len(QueueWeightsForPool(pool)) == 0 {
			t.Fatalf("worker pool %q has no queues", pool)
		}
	}
	shared := QueueWeightsForSharedPool()
	if len(shared) == 0 || shared[QueueDefault] <= 0 || shared[QueueSummary] <= 0 {
		t.Fatalf("shared pool must cover core and enrichment queues: %+v", shared)
	}
	if shared[QueuePostProcess] != 0 || shared[QueueMaintenance] != 0 {
		t.Fatalf("shared pool must not consume post-process or maintenance: %+v", shared)
	}
}

func TestChatAttachmentQueueIsIsolatedAndPrioritized(t *testing.T) {
	queue, ok := QueueForTaskType(TypeTemporaryDocumentProcess)
	if !ok || queue != QueueChatAttachment {
		t.Fatalf("temporary document parsing must use %q, got %q", QueueChatAttachment, queue)
	}
	coreWeights := QueueWeightsForPool(WorkerPoolCore)
	if coreWeights[QueueChatAttachment] <= coreWeights[QueueDefault] {
		t.Fatalf("chat attachment queue must outweigh default queue in core pool: %+v", coreWeights)
	}
	if QueueWeightsForSharedPool()[QueueChatAttachment] <= 0 {
		t.Fatalf("chat attachment queue must be eligible for shared burst capacity")
	}
}

func TestQueueMaintenanceKeepsLegacyPhysicalName(t *testing.T) {
	if QueueMaintenance != "low" {
		t.Fatalf("maintenance queue must keep legacy Redis name during rolling upgrades, got %q", QueueMaintenance)
	}
}

func TestEveryAsynqTaskTypeHasADeclaredQueue(t *testing.T) {
	taskTypes := []string{
		TypeChunkExtract, TypeDocumentProcess, TypeFAQImport,
		TypeQuestionGeneration, TypeSummaryGeneration, TypeKBClone,
		TypeIndexDelete, TypeKBDelete, TypeKnowledgeListDelete,
		TypeKnowledgeListReparse, TypeKnowledgeMove, TypeDataTableSummary,
		TypeImageMultimodal, TypeKnowledgePostProcess, TypeManualProcess,
		TypeDataSourceSync, TypeWikiIngest, TypeWikiFinalize, TypeTemporaryDocumentProcess,
	}
	for _, taskType := range taskTypes {
		if _, ok := QueueForTaskType(taskType); !ok {
			t.Fatalf("task type %q has no declared queue", taskType)
		}
	}
}

func TestDefaultWorkerPoolConcurrencyIsExplicitBudget(t *testing.T) {
	allocation := DefaultWorkerPoolConcurrency()
	if allocation.UpstreamTotal() != DefaultUpstreamWorkerConcurrency {
		t.Fatalf("upstream total = %d, want %d", allocation.UpstreamTotal(), DefaultUpstreamWorkerConcurrency)
	}
	if allocation.Core < 1 || allocation.PostProcess < 1 || allocation.Enrichment < 1 ||
		allocation.Maintenance < 1 || allocation.Shared < 1 || allocation.Wiki < 1 {
		t.Fatalf("every pool must have positive explicit capacity: %+v", allocation)
	}
}

func TestResolveWorkerPoolConcurrencyFallsBackPerPool(t *testing.T) {
	allocation := ResolveWorkerPoolConcurrency(func(key, _ string, fallback int) int {
		if key == "asynq.core_concurrency" {
			return 15
		}
		if key == "asynq.shared_concurrency" {
			return 0
		}
		return fallback
	})
	if allocation.Core != 15 {
		t.Fatalf("core = %d, want override 15", allocation.Core)
	}
	if allocation.Shared != DefaultSharedWorkerConcurrency {
		t.Fatalf("shared = %d, want fallback %d", allocation.Shared, DefaultSharedWorkerConcurrency)
	}
}
