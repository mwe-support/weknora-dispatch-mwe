package docparser

import "testing"

func TestListAllEnginesBuiltinIncludesHTML(t *testing.T) {
	engines := ListAllEngines(true, nil, nil)
	for _, engine := range engines {
		if engine.Name != "builtin" {
			continue
		}
		if !engine.Available {
			t.Fatalf("builtin engine is unavailable: %s", engine.UnavailableReason)
		}

		fileTypes := make(map[string]bool, len(engine.FileTypes))
		for _, fileType := range engine.FileTypes {
			fileTypes[fileType] = true
		}
		for _, want := range []string{"html", "htm"} {
			if !fileTypes[want] {
				t.Errorf("builtin engine file types do not include %q: %v", want, engine.FileTypes)
			}
		}
		return
	}

	t.Fatal("builtin engine not found")
}

func TestSelfHostedMinerUDoesNotAdvertiseUnsupportedLegacyPPT(t *testing.T) {
	engines := ListAllEngines(true, map[string]string{"mineru_endpoint": ""}, nil)
	for _, engine := range engines {
		if engine.Name != "mineru" {
			continue
		}
		for _, fileType := range engine.FileTypes {
			if fileType == "ppt" {
				t.Fatal("self-hosted MinerU 3.4.4 rejects legacy .ppt and must not advertise it")
			}
		}
		return
	}
	t.Fatal("mineru engine not found")
}
