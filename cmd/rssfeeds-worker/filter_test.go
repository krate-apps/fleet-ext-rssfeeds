package main

import "testing"

func TestNormalizeInterval(t *testing.T) {
	if normalizeInterval(0) != 300 {
		t.Fatalf("default")
	}
	if normalizeInterval(30) != 60 {
		t.Fatalf("min")
	}
	if normalizeInterval(120) != 120 {
		t.Fatalf("keep")
	}
}

func TestItemAllowed(t *testing.T) {
	ok, errMsg := itemAllowed("Foo.720p.Bar", "720p", "")
	if !ok || errMsg != "" {
		t.Fatalf("include ok: %v %q", ok, errMsg)
	}
	ok, errMsg = itemAllowed("Foo.1080p.Bar", "720p", "")
	if ok || errMsg != "" {
		t.Fatalf("include miss: %v %q", ok, errMsg)
	}
	ok, errMsg = itemAllowed("Foo.720p.CAM", "720p", "CAM")
	if ok || errMsg != "" {
		t.Fatalf("exclude: %v %q", ok, errMsg)
	}
	ok, errMsg = itemAllowed("Anything", "", "")
	if !ok || errMsg != "" {
		t.Fatalf("empty filters: %v %q", ok, errMsg)
	}
	_, errMsg = itemAllowed("x", "(", "")
	if errMsg == "" {
		t.Fatal("expected invalid include")
	}
}

func TestMagnetInfoHash(t *testing.T) {
	h := magnetInfoHash("magnet:?xt=urn:btih:952CF2E0BC39F1208DCC31830A7EB2BED5B02BFB&dn=x")
	if h != "952CF2E0BC39F1208DCC31830A7EB2BED5B02BFB" {
		t.Fatalf("got %q", h)
	}
	if magnetInfoHash("magnet:?dn=only") != "" {
		t.Fatal("expected empty")
	}
}
